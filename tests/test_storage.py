from __future__ import annotations

from crashsafe.models import StepStatus, WorkflowCreate, WorkflowStatus
from crashsafe.storage import SQLiteStorage


def test_seeded_workflow_is_ordered_and_uses_stable_keys(settings: object) -> None:
    database_path = settings.engine_db  # type: ignore[attr-defined]
    store = SQLiteStorage(database_path)
    workflow = store.create_workflow(
        WorkflowCreate(customer_id="customer-1", amount_cents=2500, email="a@example.com")
    )

    assert [step.name.value for step in workflow.steps] == ["charge", "provision", "notify"]
    assert len({step.operation_key for step in workflow.steps}) == 3
    assert all(step.operation_key.startswith(workflow.id) for step in workflow.steps)

    first = store.record_attempt(workflow.steps[0].id)
    assert first.status == StepStatus.INTENT_RECORDED
    assert first.attempts == 1
    assert store.next_runnable_step() is not None
    assert store.next_runnable_step().id == first.id  # type: ignore[union-attr]

    store.complete_step(first.id, {"receipt": "one"})
    second = store.next_runnable_step()
    assert second is not None and second.name.value == "provision"


def test_final_step_completion_atomically_completes_workflow(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = store.create_workflow(
        WorkflowCreate(customer_id="customer-1", amount_cents=2500, email="a@example.com")
    )
    for expected in workflow.steps:
        current = store.next_runnable_step()
        assert current is not None and current.id == expected.id
        store.record_attempt(current.id)
        store.complete_step(current.id, {"ok": True})

    finished = store.get_workflow(workflow.id)
    assert finished.status == WorkflowStatus.COMPLETED
    assert finished.completed_at is not None
    assert all(step.status == StepStatus.COMPLETED for step in finished.steps)
