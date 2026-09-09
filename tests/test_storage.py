"""SQLite transactions, migrations, scheduling, and event atomicity tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from workflow_fixtures import branched_workflow_definition, paid_workflow_definition

from crashsafe.models import EventType, StepStatus, WorkflowRunStatus
from crashsafe.storage import LegacyDatabaseError, SQLiteStorage


def test_materialized_run_persists_graph_and_stable_keys(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = store.create_workflow_run(paid_workflow_definition())

    assert run.name == "paid-customer-1"
    assert [step.id for step in run.steps] == ["charge", "provision", "notify"]
    assert [step.depends_on for step in run.steps] == [[], ["charge"], ["provision"]]
    assert [step.operation_key for step in run.steps] == [
        f"{run.run_id}:charge",
        f"{run.run_id}:provision",
        f"{run.run_id}:notify",
    ]
    created = store.list_events(run.run_id)[0]
    assert created.schema_version == 3
    assert created.payload["name"] == run.name
    assert created.payload["steps"][0]["request"]["amount_cents"] == 2500


def test_dependency_readiness_and_array_order_for_ready_branches(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = store.create_workflow_run(branched_workflow_definition())

    first = store.next_runnable_step()
    assert first is not None and first.id == "charge"
    attempted = store.record_attempt(run.run_id, first.id)
    store.complete_step(run.run_id, attempted.id, {"reference_id": "charge"})

    second = store.next_runnable_step()
    assert second is not None and second.id == "provision"
    attempted = store.record_attempt(run.run_id, second.id)
    store.complete_step(run.run_id, attempted.id, {"reference_id": "provision"})

    third = store.next_runnable_step()
    assert third is not None and third.id == "notify"


def test_completion_of_last_ready_branch_atomically_completes_run(
    settings: object,
) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = store.create_workflow_run(branched_workflow_definition())
    while store.get_workflow_run(run.run_id).status == WorkflowRunStatus.RUNNING:
        current = store.next_runnable_step()
        assert current is not None
        attempted = store.record_attempt(current.run_id, current.id)
        store.complete_step(current.run_id, attempted.id, {"ok": True})

    finished = store.get_workflow_run(run.run_id)
    assert finished.status == WorkflowRunStatus.COMPLETED
    assert finished.completed_at is not None
    assert all(step.status == StepStatus.COMPLETED for step in finished.steps)


def test_same_client_step_ids_are_reusable_across_runs(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    first = store.create_workflow_run(paid_workflow_definition("first"))
    second = store.create_workflow_run(paid_workflow_definition("second"))
    assert first.steps[0].id == second.steps[0].id == "charge"
    assert first.steps[0].operation_key != second.steps[0].operation_key


def test_v3_database_migrates_existing_execution_to_run_terminology(tmp_path: Path) -> None:
    path = tmp_path / "engine.db"
    original_store = SQLiteStorage(path)
    original = original_store.create_workflow_run(paid_workflow_definition("migration"))

    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA foreign_keys = OFF;
        BEGIN IMMEDIATE;
        DROP TRIGGER workflow_run_events_no_update;
        DROP TRIGGER workflow_run_events_no_delete;
        DROP INDEX idx_steps_runnable;
        DROP INDEX idx_workflow_run_events_order;

        UPDATE workflow_run_events
        SET event_type = CASE event_type
            WHEN 'WorkflowRunCreated' THEN 'WorkflowCreated'
            WHEN 'WorkflowRunCompleted' THEN 'WorkflowCompleted'
            WHEN 'WorkflowRunFailed' THEN 'WorkflowFailed'
            ELSE event_type
        END;
        ALTER TABLE workflow_runs RENAME COLUMN run_id TO id;
        ALTER TABLE workflow_runs RENAME TO workflows;
        ALTER TABLE steps RENAME COLUMN run_id TO workflow_id;
        ALTER TABLE step_dependencies RENAME COLUMN run_id TO workflow_id;
        ALTER TABLE workflow_run_events RENAME COLUMN run_id TO workflow_id;
        ALTER TABLE workflow_run_events RENAME TO workflow_events;
        ALTER TABLE workflow_run_leases RENAME COLUMN run_id TO workflow_id;
        ALTER TABLE workflow_run_leases RENAME TO workflow_leases;
        PRAGMA user_version = 3;
        COMMIT;
        """
    )
    connection.close()

    migrated_store = SQLiteStorage(path)
    migrated = migrated_store.get_workflow_run(original.run_id)
    assert migrated == original
    assert migrated_store.projection_matches_history(original.run_id)
    assert (
        migrated_store.list_events(original.run_id)[0].event_type == EventType.WORKFLOW_RUN_CREATED
    )

    connection = sqlite3.connect(path)
    tables = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    connection.close()
    assert version == 4
    assert {"workflow_runs", "workflow_run_events", "workflow_run_leases"} <= tables
    assert not {"workflows", "workflow_events", "workflow_leases"} & tables


def test_nonempty_pre_dag_database_requires_explicit_clean(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE workflows (id TEXT PRIMARY KEY, input_json TEXT)")
    connection.execute("INSERT INTO workflows VALUES ('old', '{}')")
    connection.commit()
    connection.close()

    with pytest.raises(LegacyDatabaseError, match="pre-DAG schema"):
        SQLiteStorage(path)
