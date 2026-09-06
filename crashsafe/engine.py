from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Optional, Protocol

import httpx

from crashsafe.config import Settings
from crashsafe.models import StepRecord, ToolResult
from crashsafe.storage import SQLiteStorage, utc_now

IDEMPOTENCY_HEADER = "Idempotency-Key"
DEBUG_DELAY_HEADER = "X-Crashsafe-Delay-After-Commit"
RETRY_AFTER_HEADER = "Retry-After"
HTTP_TOO_MANY_REQUESTS = 429
HTTP_CONFLICT = 409

logger = logging.getLogger(__name__)


class CrashPoint(str, Enum):
    BEFORE_REQUEST = "before_request"
    AFTER_RESPONSE = "after_response"
    AFTER_COMMIT = "after_commit"


class FailureInjector:
    """Deterministic hard-crash hook used only by tests and the demo."""

    def __init__(self, specification: Optional[str] = None) -> None:
        self.specification = specification or os.getenv("CRASHSAFE_CRASH_AT")

    def crash_if_requested(self, step: StepRecord, point: CrashPoint) -> None:
        if self.specification != f"{step.name.value}:{point.value}":
            return
        logger.error("injecting hard crash at %s", self.specification)
        os._exit(91)


class ToolGateway(Protocol):
    def execute(self, step: StepRecord) -> ToolResult: ...


class RetryableToolError(Exception):
    def __init__(self, message: str, retry_after_seconds: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class PermanentToolError(Exception):
    pass


class HttpToolGateway:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def execute(self, step: StepRecord) -> ToolResult:
        headers = {IDEMPOTENCY_HEADER: step.operation_key}
        delayed_step = os.getenv("CRASHSAFE_DELAY_AFTER_TOOL_COMMIT")
        if delayed_step == step.name.value:
            headers[DEBUG_DELAY_HEADER] = "true"
        try:
            response = httpx.post(
                f"{self.base_url}/tools/{step.name.value}",
                json=step.request,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except httpx.RequestError as exc:
            raise RetryableToolError(f"tool transport error: {exc}") from exc

        if response.status_code == HTTP_TOO_MANY_REQUESTS:
            retry_after = response.headers.get(RETRY_AFTER_HEADER)
            delay = float(retry_after) if retry_after is not None else None
            raise RetryableToolError("tool throttled the request", delay)
        if response.status_code == HTTP_CONFLICT:
            raise PermanentToolError("idempotency key was reused with a different request")
        if response.is_server_error:
            raise RetryableToolError(f"tool returned HTTP {response.status_code}")
        if response.is_error:
            raise PermanentToolError(f"tool returned HTTP {response.status_code}: {response.text}")
        return ToolResult.model_validate(response.json())


@dataclass(frozen=True)
class RunOutcome:
    did_work: bool
    workflow_id: Optional[str] = None
    step_name: Optional[str] = None


class WorkflowEngine:
    def __init__(
        self,
        storage: SQLiteStorage,
        gateway: ToolGateway,
        settings: Settings,
        failure_injector: Optional[FailureInjector] = None,
    ) -> None:
        self.storage = storage
        self.gateway = gateway
        self.settings = settings
        self.failure_injector = failure_injector or FailureInjector()

    def run_once(self) -> RunOutcome:
        candidate = self.storage.next_runnable_step()
        if candidate is None:
            return RunOutcome(did_work=False)

        # This FULL-synchronous SQLite commit is the durable intent boundary.
        step = self.storage.record_attempt(candidate.id)
        self.failure_injector.crash_if_requested(step, CrashPoint.BEFORE_REQUEST)
        logger.info(
            "executing workflow=%s step=%s attempt=%d key=%s",
            step.workflow_id,
            step.name.value,
            step.attempts,
            step.operation_key,
        )

        try:
            result = self.gateway.execute(step)
        except RetryableToolError as exc:
            self._handle_retryable_failure(step, exc)
            return RunOutcome(True, step.workflow_id, step.name.value)
        except PermanentToolError as exc:
            self.storage.fail_step(step.id, str(exc))
            return RunOutcome(True, step.workflow_id, step.name.value)

        self.failure_injector.crash_if_requested(step, CrashPoint.AFTER_RESPONSE)
        self.storage.complete_step(step.id, self._result_payload(result))
        self.failure_injector.crash_if_requested(step, CrashPoint.AFTER_COMMIT)
        return RunOutcome(True, step.workflow_id, step.name.value)

    def _handle_retryable_failure(self, step: StepRecord, error: RetryableToolError) -> None:
        if step.attempts >= self.settings.max_attempts:
            self.storage.fail_step(step.id, f"retry budget exhausted: {error}")
            return
        delay = error.retry_after_seconds
        if delay is None:
            delay = min(
                self.settings.base_backoff_seconds * (2 ** (step.attempts - 1)),
                self.settings.max_backoff_seconds,
            )
        self.storage.schedule_retry(step.id, str(error), utc_now() + timedelta(seconds=delay))

    @staticmethod
    def _result_payload(result: ToolResult) -> dict[str, Any]:
        return result.model_dump(mode="json")
