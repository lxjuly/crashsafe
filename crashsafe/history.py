from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar

from crashsafe.models import (
    EventPayload,
    EventType,
    StepAttemptStartedPayload,
    StepCompletedPayload,
    StepDefinition,
    StepFailedPayload,
    StepRecord,
    StepRetryScheduledPayload,
    StepStatus,
    WorkflowRun,
    WorkflowRunCompletedPayload,
    WorkflowRunCreatedPayload,
    WorkflowRunEvent,
    WorkflowRunFailedPayload,
    WorkflowRunStatus,
)

EVENT_SCHEMA_VERSION = 1
ATTEMPT_EVENT_SCHEMA_VERSION = 2
WORKFLOW_RUN_CREATED_SCHEMA_VERSION = 3
SUPPORTED_EVENT_SCHEMA_VERSIONS = {
    EVENT_SCHEMA_VERSION,
    ATTEMPT_EVENT_SCHEMA_VERSION,
    WORKFLOW_RUN_CREATED_SCHEMA_VERSION,
}
PayloadT = TypeVar("PayloadT", bound=EventPayload)

PAYLOAD_MODELS: dict[EventType, type[EventPayload]] = {
    EventType.WORKFLOW_RUN_CREATED: WorkflowRunCreatedPayload,
    EventType.STEP_ATTEMPT_STARTED: StepAttemptStartedPayload,
    EventType.STEP_RETRY_SCHEDULED: StepRetryScheduledPayload,
    EventType.STEP_COMPLETED: StepCompletedPayload,
    EventType.STEP_FAILED: StepFailedPayload,
    EventType.WORKFLOW_RUN_COMPLETED: WorkflowRunCompletedPayload,
    EventType.WORKFLOW_RUN_FAILED: WorkflowRunFailedPayload,
}


class HistoryIntegrityError(ValueError):
    pass


def parse_payload(event_type: EventType, payload: dict[str, Any]) -> EventPayload:
    return PAYLOAD_MODELS[event_type].model_validate(payload)


def canonical_payload(event_type: EventType, payload: EventPayload) -> dict[str, Any]:
    expected = PAYLOAD_MODELS[event_type]
    if not isinstance(payload, expected):
        raise TypeError(f"{event_type.value} requires {expected.__name__}")
    return payload.model_dump(mode="json")


def reduce_workflow_run_history(events: Sequence[WorkflowRunEvent]) -> WorkflowRun:
    if not events:
        raise HistoryIntegrityError("workflow run history is empty")

    run_id = events[0].run_id
    run: WorkflowRun | None = None
    expected_sequence = 1

    for event in events:
        if event.run_id != run_id:
            raise HistoryIntegrityError("history contains multiple run IDs")
        if event.sequence != expected_sequence:
            raise HistoryIntegrityError(
                f"expected event sequence {expected_sequence}, got {event.sequence}"
            )
        if event.schema_version not in SUPPORTED_EVENT_SCHEMA_VERSIONS:
            raise HistoryIntegrityError(f"unsupported event schema version {event.schema_version}")
        expected_sequence += 1

        try:
            payload = parse_payload(event.event_type, event.payload)
        except (TypeError, ValueError) as exc:
            raise HistoryIntegrityError(
                f"invalid {event.event_type.value} payload at sequence {event.sequence}"
            ) from exc

        if event.event_type == EventType.WORKFLOW_RUN_CREATED:
            if run is not None or event.sequence != 1:
                raise HistoryIntegrityError(
                    "WorkflowRunCreated must be the first and only creation"
                )
            if event.schema_version != WORKFLOW_RUN_CREATED_SCHEMA_VERSION:
                raise HistoryIntegrityError("materialized runs require WorkflowRunCreated v3")
            if event.step_id is not None or event.attempt is not None:
                raise HistoryIntegrityError("WorkflowRunCreated cannot identify a step or attempt")
            created = _require_payload(payload, WorkflowRunCreatedPayload)
            _validate_definitions(run_id, created.steps)
            run = WorkflowRun(
                run_id=run_id,
                name=created.name,
                status=WorkflowRunStatus.RUNNING,
                steps=[
                    StepRecord(
                        id=definition.id,
                        run_id=run_id,
                        position=definition.position,
                        operation=definition.operation,
                        depends_on=definition.depends_on,
                        status=StepStatus.PENDING,
                        request=definition.request,
                        operation_key=definition.operation_key,
                        tool_key=definition.tool_key,
                        attempts=0,
                        created_at=event.occurred_at,
                        updated_at=event.occurred_at,
                    )
                    for definition in created.steps
                ],
                created_at=event.occurred_at,
                updated_at=event.occurred_at,
            )
            continue

        if run is None:
            raise HistoryIntegrityError("history does not begin with WorkflowRunCreated")
        if run.status != WorkflowRunStatus.RUNNING:
            raise HistoryIntegrityError("events cannot follow a terminal run event")

        if event.event_type == EventType.WORKFLOW_RUN_COMPLETED:
            _require_payload(payload, WorkflowRunCompletedPayload)
            if event.step_id is not None or event.attempt is not None:
                raise HistoryIntegrityError(
                    "WorkflowRunCompleted cannot identify a step or attempt"
                )
            if any(step.status != StepStatus.COMPLETED for step in run.steps):
                raise HistoryIntegrityError(
                    "WorkflowRunCompleted requires every step to be completed"
                )
            run = run.model_copy(
                update={
                    "status": WorkflowRunStatus.COMPLETED,
                    "updated_at": event.occurred_at,
                    "completed_at": event.occurred_at,
                }
            )
            continue

        if event.event_type == EventType.WORKFLOW_RUN_FAILED:
            failed = _require_payload(payload, WorkflowRunFailedPayload)
            if event.step_id != failed.step_id or event.attempt is not None:
                raise HistoryIntegrityError("WorkflowRunFailed metadata does not match its payload")
            step = _find_step(run, failed.step_id)
            if step.status != StepStatus.FAILED or step.last_error != failed.error:
                raise HistoryIntegrityError("WorkflowRunFailed requires its step to be failed")
            run = run.model_copy(
                update={"status": WorkflowRunStatus.FAILED, "updated_at": event.occurred_at}
            )
            continue

        if event.step_id is None or event.attempt is None:
            raise HistoryIntegrityError(f"{event.event_type.value} requires step and attempt")
        step_index, step = _find_step_with_index(run, event.step_id)

        if event.event_type == EventType.STEP_ATTEMPT_STARTED:
            started = _require_payload(payload, StepAttemptStartedPayload)
            if event.schema_version == ATTEMPT_EVENT_SCHEMA_VERSION and (
                started.worker_id is None or started.fence_token is None
            ):
                raise HistoryIntegrityError("version 2 attempt events require lease ownership")
            if step.status not in {
                StepStatus.PENDING,
                StepStatus.INTENT_RECORDED,
                StepStatus.RETRY_WAIT,
            }:
                raise HistoryIntegrityError("attempt started from a non-runnable state")
            if event.attempt != step.attempts + 1:
                raise HistoryIntegrityError("attempt numbers must increase by exactly one")
            completed = {
                candidate.id for candidate in run.steps if candidate.status == StepStatus.COMPLETED
            }
            if not set(step.depends_on).issubset(completed):
                raise HistoryIntegrityError("attempt started before its dependencies completed")
            if (
                started.operation != step.operation
                or started.request != step.request
                or started.operation_key != step.operation_key
            ):
                raise HistoryIntegrityError("attempt changed an immutable step definition")
            step = step.model_copy(
                update={
                    "status": StepStatus.INTENT_RECORDED,
                    "attempts": event.attempt,
                    "next_attempt_at": None,
                    "last_error": None,
                    "updated_at": event.occurred_at,
                }
            )
        elif event.event_type == EventType.STEP_RETRY_SCHEDULED:
            retry = _require_payload(payload, StepRetryScheduledPayload)
            _require_current_attempt(step, event.attempt, StepStatus.INTENT_RECORDED)
            step = step.model_copy(
                update={
                    "status": StepStatus.RETRY_WAIT,
                    "last_error": retry.error,
                    "next_attempt_at": retry.next_attempt_at,
                    "updated_at": event.occurred_at,
                }
            )
        elif event.event_type == EventType.STEP_COMPLETED:
            completed_payload = _require_payload(payload, StepCompletedPayload)
            _require_current_attempt(step, event.attempt, StepStatus.INTENT_RECORDED)
            step = step.model_copy(
                update={
                    "status": StepStatus.COMPLETED,
                    "output": completed_payload.output,
                    "next_attempt_at": None,
                    "last_error": None,
                    "updated_at": event.occurred_at,
                    "completed_at": event.occurred_at,
                }
            )
        elif event.event_type == EventType.STEP_FAILED:
            failed_step = _require_payload(payload, StepFailedPayload)
            _require_current_attempt(step, event.attempt, StepStatus.INTENT_RECORDED)
            step = step.model_copy(
                update={
                    "status": StepStatus.FAILED,
                    "last_error": failed_step.error,
                    "next_attempt_at": None,
                    "updated_at": event.occurred_at,
                }
            )
        else:
            raise HistoryIntegrityError(f"unsupported event type {event.event_type.value}")

        steps = list(run.steps)
        steps[step_index] = step
        run = run.model_copy(update={"steps": steps})

    assert run is not None
    if run.status == WorkflowRunStatus.RUNNING and any(
        step.status == StepStatus.FAILED for step in run.steps
    ):
        raise HistoryIntegrityError("failed step is missing WorkflowRunFailed")
    if run.status == WorkflowRunStatus.RUNNING and all(
        step.status == StepStatus.COMPLETED for step in run.steps
    ):
        raise HistoryIntegrityError("completed steps are missing WorkflowRunCompleted")
    return run


def _validate_definitions(run_id: str, definitions: Sequence[StepDefinition]) -> None:
    if not definitions:
        raise HistoryIntegrityError("workflow definition must contain at least one step")
    positions = [definition.position for definition in definitions]
    if positions != list(range(len(definitions))):
        raise HistoryIntegrityError("step definitions must be ordered and contiguous")
    ids = [definition.id for definition in definitions]
    if len(set(ids)) != len(ids):
        raise HistoryIntegrityError("step IDs must be unique")
    known = set(ids)
    for definition in definitions:
        if definition.operation_key != f"{run_id}:{definition.id}":
            raise HistoryIntegrityError("operation key does not match run and step IDs")
        if len(set(definition.depends_on)) != len(definition.depends_on):
            raise HistoryIntegrityError("dependencies must be unique")
        if definition.id in definition.depends_on or not set(definition.depends_on) <= known:
            raise HistoryIntegrityError("step definition has an invalid dependency")
    remaining = {definition.id: set(definition.depends_on) for definition in definitions}
    visited: set[str] = set()
    while len(visited) < len(definitions):
        ready = [
            step_id for step_id in ids if step_id not in visited and remaining[step_id] <= visited
        ]
        if not ready:
            raise HistoryIntegrityError("step definitions contain a cycle")
        visited.add(ready[0])


def _find_step_with_index(run: WorkflowRun, step_id: str) -> tuple[int, StepRecord]:
    for index, step in enumerate(run.steps):
        if step.id == step_id:
            return index, step
    raise HistoryIntegrityError(f"event refers to unknown step {step_id}")


def _find_step(run: WorkflowRun, step_id: str) -> StepRecord:
    return _find_step_with_index(run, step_id)[1]


def _require_current_attempt(step: StepRecord, attempt: int, expected_status: StepStatus) -> None:
    if step.status != expected_status or step.attempts != attempt:
        raise HistoryIntegrityError("event does not match the current step attempt")


def _require_payload(payload: EventPayload, expected: type[PayloadT]) -> PayloadT:
    if not isinstance(payload, expected):
        raise HistoryIntegrityError(f"expected {expected.__name__} payload")
    return payload
