from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest
from workflow_fixtures import paid_workflow

from crashsafe.history import HistoryIntegrityError, reduce_workflow_history
from crashsafe.models import EventType, StepStatus, WorkflowEventRecord, WorkflowStatus
from crashsafe.storage import SQLiteStorage, utc_now


def create_workflow(store: SQLiteStorage) -> object:
    return store.create_workflow(paid_workflow("history-test"))


def test_event_history_is_ordered_complete_and_rebuildable(settings: object) -> None:
    path: Path = settings.engine_db  # type: ignore[attr-defined]
    store = SQLiteStorage(path)
    workflow = create_workflow(store)
    charge = workflow.steps[0]  # type: ignore[attr-defined]

    first_attempt = store.record_attempt(workflow.id, charge.id)  # type: ignore[attr-defined]
    retry_at = utc_now() + timedelta(minutes=5)
    store.schedule_retry(workflow.id, first_attempt.id, "temporary outage", retry_at)  # type: ignore[attr-defined]
    second_attempt = store.record_attempt(workflow.id, charge.id)  # type: ignore[attr-defined]
    store.complete_step(workflow.id, second_attempt.id, {"receipt": "charge-1"})  # type: ignore[attr-defined]

    for step in workflow.steps[1:]:  # type: ignore[attr-defined]
        attempted = store.record_attempt(workflow.id, step.id)  # type: ignore[attr-defined]
        store.complete_step(  # type: ignore[attr-defined]
            workflow.id, attempted.id, {"reference_id": step.operation.value}
        )

    events = store.list_events(workflow.id)  # type: ignore[attr-defined]
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert [event.event_type for event in events] == [
        EventType.WORKFLOW_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_RETRY_SCHEDULED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_COMPLETED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_COMPLETED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_COMPLETED,
        EventType.WORKFLOW_COMPLETED,
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
    assert store.projection_matches_history(workflow.id)  # type: ignore[attr-defined]

    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE steps SET attempts = 999 WHERE workflow_id = ? AND id = ?",
            (workflow.id, charge.id),  # type: ignore[attr-defined]
        )
        connection.commit()
    finally:
        connection.close()
    assert not store.projection_matches_history(workflow.id)  # type: ignore[attr-defined]

    rebuilt = store.rebuild_projection(workflow.id)  # type: ignore[attr-defined]
    assert rebuilt.status == WorkflowStatus.COMPLETED
    assert rebuilt.steps[0].attempts == 2
    assert store.projection_matches_history(workflow.id)  # type: ignore[attr-defined]


def test_event_rows_reject_update_and_delete(settings: object) -> None:
    path: Path = settings.engine_db  # type: ignore[attr-defined]
    store = SQLiteStorage(path)
    workflow = create_workflow(store)
    connection = sqlite3.connect(path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE workflow_events SET event_type = ? WHERE workflow_id = ?",
                (EventType.WORKFLOW_FAILED.value, workflow.id),  # type: ignore[attr-defined]
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "DELETE FROM workflow_events WHERE workflow_id = ?",
                (workflow.id,),  # type: ignore[attr-defined]
            )
    finally:
        connection.close()


def test_event_and_projection_roll_back_together(
    settings: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = create_workflow(store)
    step = store.record_attempt(workflow.id, workflow.steps[0].id)  # type: ignore[attr-defined]

    def fail_after_event(event_type: EventType) -> None:
        if event_type == EventType.STEP_COMPLETED:
            raise RuntimeError("injected between event and projection")

    monkeypatch.setattr(store, "_debug_pause_after_event", fail_after_event)
    with pytest.raises(RuntimeError, match="injected"):
        store.complete_step(workflow.id, step.id, {"receipt": "uncommitted"})  # type: ignore[attr-defined]

    events = store.list_events(workflow.id)  # type: ignore[attr-defined]
    assert [event.event_type for event in events] == [
        EventType.WORKFLOW_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
    ]
    interrupted = store.get_workflow(workflow.id)  # type: ignore[attr-defined]
    assert interrupted.steps[0].status == StepStatus.INTENT_RECORDED
    assert store.projection_matches_history(workflow.id)  # type: ignore[attr-defined]


def test_reducer_rejects_a_sequence_gap(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = create_workflow(store)
    event = store.list_events(workflow.id)[0]  # type: ignore[attr-defined]

    with pytest.raises(HistoryIntegrityError, match="expected event sequence"):
        reduce_workflow_history([event.model_copy(update={"sequence": 2})])


def test_reducer_rejects_attempt_before_declared_dependencies(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = create_workflow(store)
    created = store.list_events(workflow.id)[0]  # type: ignore[attr-defined]
    provision = created.payload["steps"][1]
    illegal_attempt = WorkflowEventRecord(
        id=created.id + 1,
        workflow_id=workflow.id,  # type: ignore[attr-defined]
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
        reduce_workflow_history([created, illegal_attempt])


def test_terminal_failure_is_recorded_and_rebuildable(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = create_workflow(store)
    attempted = store.record_attempt(workflow.id, workflow.steps[0].id)  # type: ignore[attr-defined]
    store.fail_step(workflow.id, attempted.id, "card declined")  # type: ignore[attr-defined]

    failed = store.get_workflow(workflow.id)  # type: ignore[attr-defined]
    assert failed.status == WorkflowStatus.FAILED
    assert failed.steps[0].status == StepStatus.FAILED
    assert [event.event_type for event in store.list_events(failed.id)] == [
        EventType.WORKFLOW_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
        EventType.STEP_FAILED,
        EventType.WORKFLOW_FAILED,
    ]
    assert store.projection_matches_history(failed.id)
