from __future__ import annotations

from dataclasses import replace

from workflow_fixtures import paid_workflow

from crashsafe.engine import RetryableToolError, WorkflowEngine
from crashsafe.models import (
    EventType,
    StepRecord,
    StepRetryScheduledPayload,
    ToolResult,
    WorkflowStatus,
)
from crashsafe.storage import SQLiteStorage, utc_now


class FlakyGateway:
    def __init__(self) -> None:
        self.keys: list[str] = []

    def execute(self, step: StepRecord) -> ToolResult:
        self.keys.append(step.operation_key)
        if len(self.keys) == 1:
            raise RetryableToolError("throttled", retry_after_seconds=0.0)
        return ToolResult(operation=step.operation, reference_id=f"ref-{step.operation.value}")


class AlwaysThrottledGateway:
    def execute(self, step: StepRecord) -> ToolResult:
        del step
        raise RetryableToolError("throttled", retry_after_seconds=30.0)


def test_retry_schedule_and_idempotency_key_survive_retry(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = store.create_workflow(paid_workflow())
    gateway = FlakyGateway()
    engine = WorkflowEngine(store, gateway, settings)  # type: ignore[arg-type]

    engine.run_once()
    waiting = store.get_workflow(workflow.id).steps[0]
    assert waiting.status.value == "retry_wait"
    assert waiting.next_attempt_at is not None
    assert waiting.next_attempt_at <= utc_now()

    engine.run_once()
    completed = store.get_workflow(workflow.id).steps[0]
    assert completed.status.value == "completed"
    assert completed.attempts == 2
    assert gateway.keys == [completed.operation_key, completed.operation_key]

    while store.get_workflow(workflow.id).status == WorkflowStatus.RUNNING:
        engine.run_once()
    assert store.get_workflow(workflow.id).status == WorkflowStatus.COMPLETED


def test_committed_retry_time_does_not_change_with_configuration(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = store.create_workflow(paid_workflow())
    engine = WorkflowEngine(store, AlwaysThrottledGateway(), settings)  # type: ignore[arg-type]
    engine.run_once()
    selected_time = store.get_workflow(workflow.id).steps[0].next_attempt_at
    assert selected_time is not None

    changed_settings = replace(  # type: ignore[arg-type]
        settings,
        base_backoff_seconds=999.0,
        max_backoff_seconds=999.0,
    )
    reopened = SQLiteStorage(changed_settings.engine_db)
    persisted = reopened.get_workflow(workflow.id).steps[0].next_attempt_at
    retry_event = reopened.list_events(workflow.id)[-1]

    assert persisted == selected_time
    assert retry_event.event_type == EventType.STEP_RETRY_SCHEDULED
    retry_payload = StepRetryScheduledPayload.model_validate(retry_event.payload)
    assert retry_payload.next_attempt_at == selected_time
