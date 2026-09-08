from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, TypeVar

from crashsafe.history import parse_payload, reduce_workflow_history
from crashsafe.models import (
    EventType,
    StepAttemptStartedPayload,
    StepCompletedPayload,
    StepFailedPayload,
    StepRetryScheduledPayload,
    TimelineEntry,
    TimelineSummary,
    WorkflowEventRecord,
    WorkflowTimeline,
)

PayloadT = TypeVar("PayloadT")


def build_timeline(events: list[WorkflowEventRecord]) -> WorkflowTimeline:
    if not events:
        raise ValueError("cannot build a timeline without events")
    rebuilt = reduce_workflow_history(events)
    operations = {step.id: step.operation for step in rebuilt.steps}
    origin = events[0].occurred_at
    entries: list[TimelineEntry] = []
    attempts = 0
    retries = 0
    planned_wait_ms = 0
    first_attempt_at: dict[str, datetime] = {}
    step_duration_ms: dict[str, int] = {}

    for event in events:
        payload = parse_payload(event.event_type, event.payload)
        values: dict[str, Any] = {
            "sequence": event.sequence,
            "elapsed_ms": _milliseconds(event.occurred_at - origin),
            "occurred_at": event.occurred_at,
            "event_type": event.event_type,
            "step_id": event.step_id,
            "operation": operations.get(event.step_id or ""),
            "attempt": event.attempt,
        }
        if event.event_type == EventType.WORKFLOW_CREATED:
            values["detail"] = f"{len(rebuilt.steps)} materialized steps persisted"
        elif event.event_type == EventType.STEP_ATTEMPT_STARTED:
            started = _as(payload, StepAttemptStartedPayload)
            attempts += 1
            values.update(
                worker_id=started.worker_id,
                fence_token=started.fence_token,
                detail=f"intent persisted; key={started.operation_key}",
            )
            if event.step_id is not None:
                first_attempt_at.setdefault(event.step_id, event.occurred_at)
        elif event.event_type == EventType.STEP_RETRY_SCHEDULED:
            retry = _as(payload, StepRetryScheduledPayload)
            retries += 1
            wait_ms = max(0, _milliseconds(retry.next_attempt_at - event.occurred_at))
            planned_wait_ms += wait_ms
            values.update(wait_ms=wait_ms, detail=retry.error)
        elif event.event_type == EventType.STEP_COMPLETED:
            completed = _as(payload, StepCompletedPayload)
            values["detail"] = _completion_detail(completed.output)
            if event.step_id is not None and event.step_id in first_attempt_at:
                step_duration_ms[event.step_id] = _milliseconds(
                    event.occurred_at - first_attempt_at[event.step_id]
                )
        elif event.event_type == EventType.STEP_FAILED:
            values["detail"] = _as(payload, StepFailedPayload).error
        elif event.event_type == EventType.WORKFLOW_COMPLETED:
            values["detail"] = "all committed steps complete"
        elif event.event_type == EventType.WORKFLOW_FAILED:
            values["detail"] = str(event.payload.get("error", "workflow failed"))
        entries.append(TimelineEntry.model_validate(values))

    return WorkflowTimeline(
        workflow_id=events[0].workflow_id,
        entries=entries,
        summary=TimelineSummary(
            status=rebuilt.status,
            duration_ms=_milliseconds(events[-1].occurred_at - origin),
            attempts=attempts,
            retries=retries,
            planned_wait_ms=planned_wait_ms,
            step_duration_ms=step_duration_ms,
        ),
    )


def format_timeline(timeline: WorkflowTimeline) -> str:
    lines = [
        f"Timeline for {timeline.workflow_id}",
        " elapsed   seq  event                 step        attempt  owner/fence  detail",
    ]
    for entry in timeline.entries:
        owner = "-" if entry.worker_id is None else f"{entry.worker_id}/{entry.fence_token}"
        detail = entry.detail or ""
        if entry.wait_ms is not None:
            detail = f"wait={entry.wait_ms}ms {detail}"
        lines.append(
            f" {entry.elapsed_ms:>7}ms {entry.sequence:>4}  "
            f"{entry.event_type.value:<21} "
            f"{(entry.step_id or '-'): <11} "
            f"{str(entry.attempt or '-'):>7}  {owner:<12} {detail}"
        )
    summary = timeline.summary
    lines.append(
        f"Summary: status={summary.status.value} duration={summary.duration_ms}ms "
        f"attempts={summary.attempts} retries={summary.retries} "
        f"planned_wait={summary.planned_wait_ms}ms"
    )
    return "\n".join(lines)


def _completion_detail(output: dict[str, Any]) -> str:
    reference = output.get("reference_id", output.get("receipt", "persisted"))
    suffix = " deduplicated" if output.get("deduplicated") else ""
    return f"result={reference}{suffix}"


def _milliseconds(delta: timedelta) -> int:
    return round(delta.total_seconds() * 1000)


def _as(value: object, expected: type[PayloadT]) -> PayloadT:
    if not isinstance(value, expected):
        raise TypeError(f"expected {expected.__name__}")
    return value
