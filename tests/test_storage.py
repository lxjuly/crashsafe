from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from workflow_fixtures import branched_workflow, paid_workflow

from crashsafe.models import StepStatus, WorkflowStatus
from crashsafe.storage import LegacyDatabaseError, SQLiteStorage


def test_materialized_workflow_persists_graph_and_stable_keys(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = store.create_workflow(paid_workflow())

    assert workflow.name == "paid-customer-1"
    assert [step.id for step in workflow.steps] == ["charge", "provision", "notify"]
    assert [step.depends_on for step in workflow.steps] == [[], ["charge"], ["provision"]]
    assert [step.operation_key for step in workflow.steps] == [
        f"{workflow.id}:charge",
        f"{workflow.id}:provision",
        f"{workflow.id}:notify",
    ]
    created = store.list_events(workflow.id)[0]
    assert created.schema_version == 3
    assert created.payload["name"] == workflow.name
    assert created.payload["steps"][0]["request"]["amount_cents"] == 2500


def test_dependency_readiness_and_array_order_for_ready_branches(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = store.create_workflow(branched_workflow())

    first = store.next_runnable_step()
    assert first is not None and first.id == "charge"
    attempted = store.record_attempt(workflow.id, first.id)
    store.complete_step(workflow.id, attempted.id, {"reference_id": "charge"})

    second = store.next_runnable_step()
    assert second is not None and second.id == "provision"
    attempted = store.record_attempt(workflow.id, second.id)
    store.complete_step(workflow.id, attempted.id, {"reference_id": "provision"})

    third = store.next_runnable_step()
    assert third is not None and third.id == "notify"


def test_completion_of_last_ready_branch_atomically_completes_workflow(
    settings: object,
) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = store.create_workflow(branched_workflow())
    while store.get_workflow(workflow.id).status == WorkflowStatus.RUNNING:
        current = store.next_runnable_step()
        assert current is not None
        attempted = store.record_attempt(current.workflow_id, current.id)
        store.complete_step(current.workflow_id, attempted.id, {"ok": True})

    finished = store.get_workflow(workflow.id)
    assert finished.status == WorkflowStatus.COMPLETED
    assert finished.completed_at is not None
    assert all(step.status == StepStatus.COMPLETED for step in finished.steps)


def test_same_client_step_ids_are_reusable_across_workflows(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    first = store.create_workflow(paid_workflow("first"))
    second = store.create_workflow(paid_workflow("second"))
    assert first.steps[0].id == second.steps[0].id == "charge"
    assert first.steps[0].operation_key != second.steps[0].operation_key


def test_nonempty_pre_dag_database_requires_explicit_clean(tmp_path: Path) -> None:
    path = tmp_path / "legacy.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE workflows (id TEXT PRIMARY KEY, input_json TEXT)")
    connection.execute("INSERT INTO workflows VALUES ('old', '{}')")
    connection.commit()
    connection.close()

    with pytest.raises(LegacyDatabaseError, match="pre-DAG schema"):
        SQLiteStorage(path)
