from __future__ import annotations

from workflow_fixtures import paid_workflow_definition

from crashsafe.engine import RetryableToolError, WorkflowEngine
from crashsafe.models import StepRecord, ToolResult, WorkflowRunStatus
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
    run = store.create_workflow_run(paid_workflow_definition("one"))
    engine = WorkflowEngine(
        store,
        RetryOnceGateway(),
        settings,
        worker_id="timeline-worker",  # type: ignore[arg-type]
    )
    while store.get_workflow_run(run.run_id).status == WorkflowRunStatus.RUNNING:
        engine.run_once()

    timeline = build_timeline(store.list_events(run.run_id))
    rendered = format_timeline(timeline)

    assert timeline.summary.status == WorkflowRunStatus.COMPLETED
    assert timeline.summary.attempts == 4
    assert timeline.summary.retries == 1
    assert store.projection_matches_history(run.run_id)
    assert timeline.entries[1].worker_id == "timeline-worker"
    assert timeline.entries[1].fence_token == 1
    assert "↻ RETRY SCHEDULED" in rendered
    assert "✓ COMPLETED" in rendered
    assert f"│ ID       {run.run_id}" in rendered
    assert "PLANNED WAIT" in rendered
    assert "timeline-worker · f1" in rendered
    assert "╭─ WORKFLOW RUN" in rendered
    assert rendered.endswith("─")
    assert "audit" not in rendered
