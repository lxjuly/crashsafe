from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class WorkflowStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepStatus(str, Enum):
    PENDING = "pending"
    INTENT_RECORDED = "intent_recorded"
    RETRY_WAIT = "retry_wait"
    COMPLETED = "completed"
    FAILED = "failed"


class StepName(str, Enum):
    CHARGE = "charge"
    PROVISION = "provision"
    NOTIFY = "notify"


class EventType(str, Enum):
    WORKFLOW_CREATED = "WorkflowCreated"
    STEP_ATTEMPT_STARTED = "StepAttemptStarted"
    STEP_RETRY_SCHEDULED = "StepRetryScheduled"
    STEP_COMPLETED = "StepCompleted"
    STEP_FAILED = "StepFailed"
    WORKFLOW_COMPLETED = "WorkflowCompleted"
    WORKFLOW_FAILED = "WorkflowFailed"


class WorkflowCreate(BaseModel):
    customer_id: str = Field(min_length=1, max_length=128)
    amount_cents: int = Field(gt=0)
    email: str = Field(min_length=3, max_length=320)


class EventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class StepDefinition(EventPayload):
    id: str
    position: int
    name: StepName
    request: dict[str, Any]
    operation_key: str
    tool_key: str = "mock-tool"


class WorkflowCreatedPayload(EventPayload):
    input: WorkflowCreate
    steps: list[StepDefinition]


class StepAttemptStartedPayload(EventPayload):
    name: StepName
    request: dict[str, Any]
    operation_key: str
    worker_id: Optional[str] = None
    fence_token: Optional[int] = None


class StepRetryScheduledPayload(EventPayload):
    error: str
    next_attempt_at: datetime


class StepCompletedPayload(EventPayload):
    output: dict[str, Any]


class StepFailedPayload(EventPayload):
    error: str


class WorkflowCompletedPayload(EventPayload):
    pass


class WorkflowFailedPayload(EventPayload):
    step_id: str
    error: str


class StepRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    workflow_id: str
    position: int
    name: StepName
    status: StepStatus
    request: dict[str, Any]
    operation_key: str
    tool_key: str = "mock-tool"
    output: Optional[dict[str, Any]] = None
    attempts: int
    next_attempt_at: Optional[datetime] = None
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None


class WorkflowRecord(BaseModel):
    id: str
    status: WorkflowStatus
    input: WorkflowCreate
    steps: list[StepRecord]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None


class WorkflowEventRecord(BaseModel):
    id: int
    workflow_id: str
    sequence: int
    event_type: EventType
    schema_version: int
    step_id: Optional[str] = None
    attempt: Optional[int] = None
    payload: dict[str, Any]
    occurred_at: datetime


class WorkflowAudit(BaseModel):
    consistent: bool
    projected: WorkflowRecord
    rebuilt: WorkflowRecord


class WorkflowLease(BaseModel):
    workflow_id: str
    owner_id: str
    fence_token: int
    lease_expires_at: datetime
    updated_at: datetime


class ClaimedStep(BaseModel):
    step: StepRecord
    lease: WorkflowLease


class ToolThrottle(BaseModel):
    tool_key: str
    blocked_until: datetime
    reason: str
    updated_at: datetime


class TimelineEntry(BaseModel):
    sequence: int
    elapsed_ms: int
    occurred_at: datetime
    event_type: EventType
    step: Optional[StepName] = None
    attempt: Optional[int] = None
    worker_id: Optional[str] = None
    fence_token: Optional[int] = None
    wait_ms: Optional[int] = None
    detail: Optional[str] = None


class TimelineSummary(BaseModel):
    status: WorkflowStatus
    duration_ms: int
    attempts: int
    retries: int
    planned_wait_ms: int
    audit_consistent: bool
    step_duration_ms: dict[str, int]


class WorkflowTimeline(BaseModel):
    workflow_id: str
    entries: list[TimelineEntry]
    summary: TimelineSummary


class ToolResult(BaseModel):
    operation: StepName
    reference_id: str
    deduplicated: bool = False


class LedgerSummary(BaseModel):
    charges: int
    provisions: int
    notifications: int
