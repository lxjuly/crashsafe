from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest
from workflow_fixtures import paid_workflow_definition

from crashsafe.history import HistoryIntegrityError, reduce_workflow_run_history
from crashsafe.models import EventType, StepStatus, WorkflowRun, WorkflowRunEvent, WorkflowRunStatus
from crashsafe.storage import SQLiteStorage, utc_now


def create_workflow_run(store: SQLiteStorage) -> WorkflowRun:
    return store.create_workflow_run(paid_workflow_definition("history-test"))


def test_event_history_is_ordered_complete_and_rebuildable(settings: object) -> None:
    path: Path = settings.engine_db  # type: ignore[attr-defined]
    store = SQLiteStorage(path)
    run = create_workflow_run(store)
    charge = run.steps[0]  # type: ignore[attr-defined]

    first_attempt = store.record_attempt(run.run_id, charge.id)
    retry_at = utc_now() + timedelta(minutes=5)
    store.schedule_retry(run.run_id, first_attempt.id, "temporary outage", retry_at)
    second_attempt = store.record_attempt(run.run_id, charge.id)
    store.complete_step(run.run_id, second_attempt.id, {"receipt": "charge-1"})

    for step in run.steps[1:]:  # type: ignore[attr-defined]
        attempted = store.record_attempt(run.run_id, step.id)
        store.complete_step(  # type: ignore[attr-defined]
            run.run_id, attempted.id, {"reference_id": step.operation.value}
        )

    events = store.list_events(run.run_id)
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert [event.event_type for event in events] == [
        EventType.WORKFLOW_RUN_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_RETRY_SCHEDULED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_COMPLETED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_COMPLETED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_COMPLETED,
        EventType.WORKFLOW_RUN_COMPLETED,
    ]
    charge_attempts = [
        event
        for event in events
        if event.event_type == EventType.STEP_ATTEMPT_STARTED and event.step_id == charge.id
    ]
    assert [event.attempt for event in charge_attempts] == [1, 2]
    assert {str(event.payload["operation_key"]) for event in charge_attempts} == {
        charge.operation_key
    }
    assert store.projection_matches_history(run.run_id)

    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE steps SET attempts = 999 WHERE run_id = ? AND id = ?",
            (run.run_id, charge.id),
        )
        connection.commit()
    finally:
        connection.close()
    assert not store.projection_matches_history(run.run_id)

    rebuilt = store.rebuild_projection(run.run_id)
    assert rebuilt.status == WorkflowRunStatus.COMPLETED
    assert rebuilt.steps[0].attempts == 2
    assert store.projection_matches_history(run.run_id)


def test_event_rows_reject_update_and_delete(settings: object) -> None:
    path: Path = settings.engine_db  # type: ignore[attr-defined]
    store = SQLiteStorage(path)
    run = create_workflow_run(store)
    connection = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE workflow_run_events SET event_type = ? WHERE run_id = ?",
                (EventType.WORKFLOW_RUN_FAILED.value, run.run_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "DELETE FROM workflow_run_events WHERE run_id = ?",
                (run.run_id,),
            )
    finally:
        connection.close()


def test_event_and_projection_roll_back_together(
    settings: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = create_workflow_run(store)
    step = store.record_attempt(run.run_id, run.steps[0].id)

    def fail_after_event(event_type: EventType) -> None:
        if event_type == EventType.STEP_COMPLETED:
            raise RuntimeError("injected between event and projection")

    monkeypatch.setattr(store, "_debug_pause_after_event", fail_after_event)
    with pytest.raises(RuntimeError, match="injected"):
        store.complete_step(run.run_id, step.id, {"receipt": "uncommitted"})

    events = store.list_events(run.run_id)
    assert [event.event_type for event in events] == [
        EventType.WORKFLOW_RUN_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
    ]
    interrupted = store.get_workflow_run(run.run_id)
    assert interrupted.steps[0].status == StepStatus.INTENT_RECORDED
    assert store.projection_matches_history(run.run_id)


def test_reducer_rejects_a_sequence_gap(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = create_workflow_run(store)
    event = store.list_events(run.run_id)[0]

    with pytest.raises(HistoryIntegrityError, match="expected event sequence"):
        reduce_workflow_run_history([event.model_copy(update={"sequence": 2})])


def test_reducer_rejects_attempt_before_declared_dependencies(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = create_workflow_run(store)
    created = store.list_events(run.run_id)[0]
    provision = created.payload["steps"][1]
    illegal_attempt = WorkflowRunEvent(
        id=created.id + 1,
        run_id=run.run_id,
        sequence=2,
        event_type=EventType.STEP_ATTEMPT_STARTED,
        schema_version=1,
        step_id="provision",
        attempt=1,
        payload={
            "operation": provision["operation"],
            "request": provision["request"],
            "operation_key": provision["operation_key"],
        },
        occurred_at=created.occurred_at,
    )

    with pytest.raises(HistoryIntegrityError, match="dependencies"):
        reduce_workflow_run_history([created, illegal_attempt])


def test_terminal_failure_is_recorded_and_rebuildable(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = create_workflow_run(store)
    attempted = store.record_attempt(run.run_id, run.steps[0].id)
    store.fail_step(run.run_id, attempted.id, "card declined")

    failed = store.get_workflow_run(run.run_id)
    assert failed.status == WorkflowRunStatus.FAILED
    assert failed.steps[0].status == StepStatus.FAILED
    assert [event.event_type for event in store.list_events(failed.run_id)] == [
        EventType.WORKFLOW_RUN_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_FAILED,
        EventType.WORKFLOW_RUN_FAILED,
    ]
    assert store.projection_matches_history(failed.run_id)
