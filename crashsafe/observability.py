"""History-derived timeline construction and terminal-friendly rendering.

Timeline data is never a second persistence model: each view is rebuilt from the
same ordered events used to verify the scheduling projection.
"""

from __future__ import annotations

import shutil
import textwrap
from datetime import datetime, timedelta
from typing import Any, Optional, TypeVar

from crashsafe.history import parse_payload, reduce_workflow_run_history
from crashsafe.models import (
    EventType,
    StepAttemptStartedPayload,
    StepCompletedPayload,
    StepFailedPayload,
    StepRetryScheduledPayload,
    TimelineEntry,
    TimelineSummary,
    WorkflowRunEvent,
    WorkflowRunTimeline,
)

PayloadT = TypeVar("PayloadT")


def build_timeline(events: list[WorkflowRunEvent]) -> WorkflowRunTimeline:
    """Build a read-only operational view from the same history used for recovery."""
    if not events:
        raise ValueError("cannot build a timeline without events")
    rebuilt = reduce_workflow_run_history(events)
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
        if event.event_type == EventType.WORKFLOW_RUN_CREATED:
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
        elif event.event_type == EventType.WORKFLOW_RUN_COMPLETED:
            values["detail"] = "all committed steps complete"
        elif event.event_type == EventType.WORKFLOW_RUN_FAILED:
            values["detail"] = str(event.payload.get("error", "workflow run failed"))
        entries.append(TimelineEntry.model_validate(values))

    return WorkflowRunTimeline(
        run_id=events[0].run_id,
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


def format_timeline(timeline: WorkflowRunTimeline, width: Optional[int] = None) -> str:
    """Render a responsive plain-terminal timeline for demos and manual review."""
    terminal_width = shutil.get_terminal_size(fallback=(120, 24)).columns
    width = max(72, width if width is not None else terminal_width - 1)
    summary = timeline.summary
    status_symbol = "✓" if summary.status.value == "completed" else "●"
    lines = [_box_title("WORKFLOW RUN", width)]
    _append_box_line(lines, f"ID       {timeline.run_id}", width)
    _append_box_line(
        lines,
        (
            f"STATUS   {status_symbol} {summary.status.value.upper():<9} "
            f"DURATION  {_format_duration(summary.duration_ms):<8} "
            f"ATTEMPTS  {summary.attempts:<3}  RETRIES  {summary.retries:<3}  "
            f"PLANNED WAIT  {_format_duration(summary.planned_wait_ms)}"
        ),
        width,
    )
    lines.append(_box_separator(width))
    _append_box_line(
        lines,
        "TIME       #   EVENT                 STEP                TRY   WORKER · FENCE",
        width,
    )
    lines.append(_box_separator(width))
    for entry in timeline.entries:
        symbol, event_label = _event_display(entry.event_type)
        owner = "—" if entry.worker_id is None else f"{entry.worker_id} · f{entry.fence_token}"
        attempt = "—" if entry.attempt is None else f"#{entry.attempt}"
        _append_box_line(
            lines,
            f"+{_format_duration(entry.elapsed_ms):<9} {entry.sequence:>2}  "
            f"{symbol} {event_label:<19} {(entry.step_id or 'run'):<19} "
            f"{attempt:<5} {owner}",
            width,
        )
        for index, detail in enumerate(_timeline_details(entry.detail, entry.wait_ms)):
            branch = "↳" if index == 0 else " "
            _append_box_line(lines, f"             {branch} {detail}", width)
    lines.append("╰" + "─" * (width - 2) + "╯")
    return "\n".join(lines)


def _box_title(title: str, width: int) -> str:
    label = f"─ {title} "
    return "╭" + label + "─" * (width - len(label) - 2) + "╮"


def _box_separator(width: int) -> str:
    return "├" + "─" * (width - 2) + "┤"


def _append_box_line(lines: list[str], content: str, width: int) -> None:
    content_width = width - 4
    wrapped = (
        [content]
        if len(content) <= content_width
        else textwrap.wrap(content, width=content_width, subsequent_indent="  ")
    )
    for part in wrapped or [""]:
        lines.append(f"│ {part:<{content_width}} │")


def _event_display(event_type: EventType) -> tuple[str, str]:
    return {
        EventType.WORKFLOW_RUN_CREATED: ("◇", "RUN CREATED"),
        EventType.STEP_ATTEMPT_STARTED: ("●", "ATTEMPT STARTED"),
        EventType.STEP_RETRY_SCHEDULED: ("↻", "RETRY SCHEDULED"),
        EventType.STEP_COMPLETED: ("✓", "STEP COMPLETED"),
        EventType.STEP_FAILED: ("✕", "STEP FAILED"),
        EventType.WORKFLOW_RUN_COMPLETED: ("◆", "RUN COMPLETED"),
        EventType.WORKFLOW_RUN_FAILED: ("✕", "RUN FAILED"),
    }[event_type]


def _timeline_details(detail: Optional[str], wait_ms: Optional[int]) -> list[str]:
    values: list[str] = []
    if wait_ms is not None:
        values.append(f"wait {_format_duration(wait_ms)}")
    if detail:
        if "; key=" in detail:
            message, key = detail.split("; key=", maxsplit=1)
            values.extend([message, f"key  {key}"])
        elif values:
            values[0] = f"{values[0]} · {detail}"
        else:
            values.append(detail)
    return values


def _format_duration(milliseconds: int) -> str:
    if milliseconds < 1000:
        return f"{milliseconds}ms"
    return f"{milliseconds / 1000:.3f}s"


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
