from __future__ import annotations

from workflow_fixtures import paid_workflow

from crashsafe.engine import RetryableToolError, WorkflowEngine
from crashsafe.models import StepRecord, ToolResult, WorkflowStatus
from crashsafe.observability import build_timeline, format_timeline
from crashsafe.storage import SQLiteStorage


class RetryOnceGateway:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, step: StepRecord) -> ToolResult:
        self.calls += 1
        if self.calls == 1:
            raise RetryableToolError("brief outage", retry_after_seconds=0.0)
        return ToolResult(operation=step.operation, reference_id=f"ref-{step.operation.value}")


def test_timeline_is_derived_from_history(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    workflow = store.create_workflow(paid_workflow("one"))
    engine = WorkflowEngine(
        store,
        RetryOnceGateway(),
        settings,
        worker_id="timeline-worker",  # type: ignore[arg-type]
    )
    while store.get_workflow(workflow.id).status == WorkflowStatus.RUNNING:
        engine.run_once()

    timeline = build_timeline(store.list_events(workflow.id))
    rendered = format_timeline(timeline)

    assert timeline.summary.status == WorkflowStatus.COMPLETED
    assert timeline.summary.attempts == 4
    assert timeline.summary.retries == 1
    assert store.projection_matches_history(workflow.id)
    assert timeline.entries[1].worker_id == "timeline-worker"
    assert timeline.entries[1].fence_token == 1
    assert "StepRetryScheduled" in rendered
    assert "audit" not in rendered
