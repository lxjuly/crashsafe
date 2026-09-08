from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


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


class ChargeRequest(StrictModel):
    customer_id: str = Field(min_length=1, max_length=128)
    amount_cents: int = Field(gt=0)


class ProvisionRequest(StrictModel):
    customer_id: str = Field(min_length=1, max_length=128)
    plan: str = Field(min_length=1, max_length=128)


class NotifyRequest(StrictModel):
    customer_id: str = Field(min_length=1, max_length=128)
    email: str = Field(min_length=3, max_length=320)
    message: str = Field(min_length=1, max_length=1000)


class WorkflowStepBase(StrictModel):
    id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    depends_on: list[str] = Field(default_factory=list, max_length=32)


class ChargeStep(WorkflowStepBase):
    operation: Literal[StepName.CHARGE]
    request: ChargeRequest


class ProvisionStep(WorkflowStepBase):
    operation: Literal[StepName.PROVISION]
    request: ProvisionRequest


class NotifyStep(WorkflowStepBase):
    operation: Literal[StepName.NOTIFY]
    request: NotifyRequest


WorkflowStep = Annotated[
    Union[ChargeStep, ProvisionStep, NotifyStep], Field(discriminator="operation")
]


class WorkflowCreate(StrictModel):
    name: str = Field(min_length=1, max_length=128)
    steps: list[WorkflowStep] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def validate_graph(self) -> WorkflowCreate:
        ids = [step.id for step in self.steps]
        if len(set(ids)) != len(ids):
            raise ValueError("step IDs must be unique")
        known = set(ids)
        for step in self.steps:
            if len(set(step.depends_on)) != len(step.depends_on):
                raise ValueError(f"step {step.id} has duplicate dependencies")
            if step.id in step.depends_on:
                raise ValueError(f"step {step.id} cannot depend on itself")
            missing = set(step.depends_on) - known
            if missing:
                raise ValueError(f"step {step.id} has unknown dependencies: {sorted(missing)}")

        dependencies = {step.id: set(step.depends_on) for step in self.steps}
        ready = [step_id for step_id in ids if not dependencies[step_id]]
        visited: list[str] = []
        while ready:
            current = ready.pop(0)
            visited.append(current)
            for candidate in ids:
                if current in dependencies[candidate]:
                    dependencies[candidate].remove(current)
                    if (
                        not dependencies[candidate]
                        and candidate not in visited
                        and candidate not in ready
                    ):
                        ready.append(candidate)
        if len(visited) != len(ids):
            raise ValueError("workflow dependencies must be acyclic")
        return self


class EventPayload(StrictModel):
    pass


class StepDefinition(EventPayload):
    id: str
    position: int
    operation: StepName
    depends_on: list[str]
    request: dict[str, Any]
    operation_key: str
    tool_key: str = "mock-tool"


class WorkflowCreatedPayload(EventPayload):
    name: str
    steps: list[StepDefinition]


class StepAttemptStartedPayload(EventPayload):
    operation: StepName
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
    operation: StepName
    depends_on: list[str]
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
    name: str
    status: WorkflowStatus
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
    step_id: Optional[str] = None
    operation: Optional[StepName] = None
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
