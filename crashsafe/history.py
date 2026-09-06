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
    WorkflowCompletedPayload,
    WorkflowCreatedPayload,
    WorkflowEventRecord,
    WorkflowFailedPayload,
    WorkflowRecord,
    WorkflowStatus,
)

EVENT_SCHEMA_VERSION = 1
PayloadT = TypeVar("PayloadT", bound=EventPayload)

PAYLOAD_MODELS: dict[EventType, type[EventPayload]] = {
    EventType.WORKFLOW_CREATED: WorkflowCreatedPayload,
    EventType.STEP_ATTEMPT_STARTED: StepAttemptStartedPayload,
    EventType.STEP_RETRY_SCHEDULED: StepRetryScheduledPayload,
    EventType.STEP_COMPLETED: StepCompletedPayload,
    EventType.STEP_FAILED: StepFailedPayload,
    EventType.WORKFLOW_COMPLETED: WorkflowCompletedPayload,
    EventType.WORKFLOW_FAILED: WorkflowFailedPayload,
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


def reduce_workflow_history(events: Sequence[WorkflowEventRecord]) -> WorkflowRecord:
    if not events:
        raise HistoryIntegrityError("workflow history is empty")

    workflow_id = events[0].workflow_id
    workflow: WorkflowRecord | None = None
    expected_sequence = 1

    for event in events:
        if event.workflow_id != workflow_id:
            raise HistoryIntegrityError("history contains multiple workflow IDs")
        if event.sequence != expected_sequence:
            raise HistoryIntegrityError(
                f"expected event sequence {expected_sequence}, got {event.sequence}"
            )
        if event.schema_version != EVENT_SCHEMA_VERSION:
            raise HistoryIntegrityError(
                f"unsupported event schema version {event.schema_version}"
            )
        expected_sequence += 1

        try:
            payload = parse_payload(event.event_type, event.payload)
        except (TypeError, ValueError) as exc:
            raise HistoryIntegrityError(
                f"invalid {event.event_type.value} payload at sequence {event.sequence}"
            ) from exc

        if event.event_type == EventType.WORKFLOW_CREATED:
            if workflow is not None or event.sequence != 1:
                raise HistoryIntegrityError("WorkflowCreated must be the first and only creation")
            if event.step_id is not None or event.attempt is not None:
                raise HistoryIntegrityError("WorkflowCreated cannot identify a step or attempt")
            created = _require_payload(payload, WorkflowCreatedPayload)
            _validate_definitions(created.steps)
            workflow = WorkflowRecord(
                id=workflow_id,
                status=WorkflowStatus.RUNNING,
                input=created.input,
                steps=[
                    StepRecord(
                        id=definition.id,
                        workflow_id=workflow_id,
                        position=definition.position,
                        name=definition.name,
                        status=StepStatus.PENDING,
                        request=definition.request,
                        operation_key=definition.operation_key,
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

        if workflow is None:
            raise HistoryIntegrityError("history does not begin with WorkflowCreated")
        if workflow.status != WorkflowStatus.RUNNING:
            raise HistoryIntegrityError("events cannot follow a terminal workflow event")

        if event.event_type == EventType.WORKFLOW_COMPLETED:
            _require_payload(payload, WorkflowCompletedPayload)
            if event.step_id is not None or event.attempt is not None:
                raise HistoryIntegrityError("WorkflowCompleted cannot identify a step or attempt")
            if any(step.status != StepStatus.COMPLETED for step in workflow.steps):
                raise HistoryIntegrityError("WorkflowCompleted requires every step to be completed")
            workflow = workflow.model_copy(
                update={
                    "status": WorkflowStatus.COMPLETED,
                    "updated_at": event.occurred_at,
                    "completed_at": event.occurred_at,
                }
            )
            continue

        if event.event_type == EventType.WORKFLOW_FAILED:
            failed = _require_payload(payload, WorkflowFailedPayload)
            if event.step_id != failed.step_id or event.attempt is not None:
                raise HistoryIntegrityError("WorkflowFailed metadata does not match its payload")
            step = _find_step(workflow, failed.step_id)
            if step.status != StepStatus.FAILED or step.last_error != failed.error:
                raise HistoryIntegrityError("WorkflowFailed requires its step to be failed")
            workflow = workflow.model_copy(
                update={"status": WorkflowStatus.FAILED, "updated_at": event.occurred_at}
            )
            continue

        if event.step_id is None or event.attempt is None:
            raise HistoryIntegrityError(f"{event.event_type.value} requires step and attempt")
        step_index, step = _find_step_with_index(workflow, event.step_id)

        if event.event_type == EventType.STEP_ATTEMPT_STARTED:
            started = _require_payload(payload, StepAttemptStartedPayload)
            if step.status not in {
                StepStatus.PENDING,
                StepStatus.INTENT_RECORDED,
                StepStatus.RETRY_WAIT,
            }:
                raise HistoryIntegrityError("attempt started from a non-runnable state")
            if event.attempt != step.attempts + 1:
                raise HistoryIntegrityError("attempt numbers must increase by exactly one")
            if any(
                previous.position < step.position
                and previous.status != StepStatus.COMPLETED
                for previous in workflow.steps
            ):
                raise HistoryIntegrityError("attempt started before its predecessors completed")
            if (
                started.name != step.name
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
            completed = _require_payload(payload, StepCompletedPayload)
            _require_current_attempt(step, event.attempt, StepStatus.INTENT_RECORDED)
            step = step.model_copy(
                update={
                    "status": StepStatus.COMPLETED,
                    "output": completed.output,
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

        steps = list(workflow.steps)
        steps[step_index] = step
        workflow = workflow.model_copy(update={"steps": steps})

    assert workflow is not None
    if workflow.status == WorkflowStatus.RUNNING and any(
        step.status == StepStatus.FAILED for step in workflow.steps
    ):
        raise HistoryIntegrityError("failed step is missing WorkflowFailed")
    if workflow.status == WorkflowStatus.RUNNING and all(
        step.status == StepStatus.COMPLETED for step in workflow.steps
    ):
        raise HistoryIntegrityError("completed steps are missing WorkflowCompleted")
    return workflow


def _validate_definitions(definitions: Sequence[StepDefinition]) -> None:
    positions = [definition.position for definition in definitions]
    if positions != list(range(len(definitions))):
        raise HistoryIntegrityError("step definitions must be ordered and contiguous")
    if len({definition.id for definition in definitions}) != len(definitions):
        raise HistoryIntegrityError("step IDs must be unique")
    if len({definition.operation_key for definition in definitions}) != len(definitions):
        raise HistoryIntegrityError("operation keys must be unique")


def _find_step_with_index(workflow: WorkflowRecord, step_id: str) -> tuple[int, StepRecord]:
    for index, step in enumerate(workflow.steps):
        if step.id == step_id:
            return index, step
    raise HistoryIntegrityError(f"event refers to unknown step {step_id}")


def _find_step(workflow: WorkflowRecord, step_id: str) -> StepRecord:
    return _find_step_with_index(workflow, step_id)[1]


def _require_current_attempt(
    step: StepRecord, attempt: int, expected_status: StepStatus
) -> None:
    if step.status != expected_status or step.attempts != attempt:
        raise HistoryIntegrityError("event does not match the current step attempt")


def _require_payload(payload: EventPayload, expected: type[PayloadT]) -> PayloadT:
    if not isinstance(payload, expected):
        raise HistoryIntegrityError(f"expected {expected.__name__} payload")
    return payload
