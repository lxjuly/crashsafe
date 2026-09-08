from __future__ import annotations

import logging
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional, Protocol

import httpx

from crashsafe.config import Settings
from crashsafe.models import StepRecord, ToolResult
from crashsafe.storage import LeaseLostError, SQLiteStorage, utc_now

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
    def __init__(
        self,
        message: str,
        retry_after_seconds: Optional[float] = None,
        *,
        throttle_tool: bool = False,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds
        self.throttle_tool = throttle_tool


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
            try:
                delay = float(retry_after) if retry_after is not None else None
            except ValueError:
                delay = None
            raise RetryableToolError(
                "tool throttled the request", delay, throttle_tool=True
            )
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


class LeaseHeartbeat:
    def __init__(
        self,
        storage: SQLiteStorage,
        workflow_id: str,
        owner_id: str,
        fence_token: int,
        ttl_seconds: float,
        interval_seconds: float,
    ) -> None:
        self.storage = storage
        self.workflow_id = workflow_id
        self.owner_id = owner_id
        self.fence_token = fence_token
        self.ttl_seconds = ttl_seconds
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"lease-heartbeat-{workflow_id[:8]}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self.interval_seconds + 1.0)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                renewed = self.storage.renew_lease(
                    self.workflow_id,
                    self.owner_id,
                    self.fence_token,
                    self.ttl_seconds,
                )
            except Exception:
                logger.exception("lease heartbeat failed for workflow=%s", self.workflow_id)
                continue
            if not renewed:
                logger.warning(
                    "lease lost workflow=%s worker=%s fence=%d",
                    self.workflow_id,
                    self.owner_id,
                    self.fence_token,
                )
                return


class WorkflowEngine:
    def __init__(
        self,
        storage: SQLiteStorage,
        gateway: ToolGateway,
        settings: Settings,
        failure_injector: Optional[FailureInjector] = None,
        worker_id: Optional[str] = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.storage = storage
        self.gateway = gateway
        self.settings = settings
        self.failure_injector = failure_injector or FailureInjector()
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.clock = clock

    def run_once(self) -> RunOutcome:
        claim = self.storage.claim_runnable_step(
            self.worker_id, self.settings.lease_ttl_seconds, now=self.clock()
        )
        if claim is None:
            return RunOutcome(did_work=False)

        lease = claim.lease
        heartbeat: Optional[LeaseHeartbeat] = None
        try:
            # This FULL-synchronous SQLite commit is the durable intent boundary.
            step = self.storage.record_attempt(
                claim.step.id, lease.owner_id, lease.fence_token
            )
            heartbeat = LeaseHeartbeat(
                self.storage,
                step.workflow_id,
                lease.owner_id,
                lease.fence_token,
                self.settings.lease_ttl_seconds,
                self.settings.lease_renew_interval_seconds,
            )
            heartbeat.start()
            self.failure_injector.crash_if_requested(step, CrashPoint.BEFORE_REQUEST)
            logger.info(
                "executing workflow=%s step=%s attempt=%d key=%s worker=%s fence=%d",
                step.workflow_id,
                step.name.value,
                step.attempts,
                step.operation_key,
                lease.owner_id,
                lease.fence_token,
            )

            try:
                result = self.gateway.execute(step)
            except RetryableToolError as exc:
                self._handle_retryable_failure(step, exc, lease.owner_id, lease.fence_token)
                return RunOutcome(True, step.workflow_id, step.name.value)
            except PermanentToolError as exc:
                self.storage.fail_step(
                    step.id, str(exc), lease.owner_id, lease.fence_token
                )
                return RunOutcome(True, step.workflow_id, step.name.value)

            self.failure_injector.crash_if_requested(step, CrashPoint.AFTER_RESPONSE)
            self.storage.complete_step(
                step.id,
                self._result_payload(result),
                lease.owner_id,
                lease.fence_token,
            )
            self.failure_injector.crash_if_requested(step, CrashPoint.AFTER_COMMIT)
            return RunOutcome(True, step.workflow_id, step.name.value)
        except LeaseLostError:
            logger.warning(
                "discarding stale result workflow=%s worker=%s fence=%d",
                claim.step.workflow_id,
                lease.owner_id,
                lease.fence_token,
            )
            return RunOutcome(True, claim.step.workflow_id, claim.step.name.value)
        finally:
            if heartbeat is not None:
                heartbeat.stop()
            self.storage.release_lease(
                claim.step.workflow_id, lease.owner_id, lease.fence_token
            )

    def _handle_retryable_failure(
        self,
        step: StepRecord,
        error: RetryableToolError,
        owner_id: str,
        fence_token: int,
    ) -> None:
        if step.attempts >= self.settings.max_attempts:
            self.storage.fail_step(
                step.id,
                f"retry budget exhausted: {error}",
                owner_id,
                fence_token,
            )
            return
        delay = error.retry_after_seconds
        if delay is None:
            delay = min(
                self.settings.base_backoff_seconds * (2 ** (step.attempts - 1)),
                self.settings.max_backoff_seconds,
            )
        self.storage.schedule_retry(
            step.id,
            str(error),
            self.clock() + timedelta(seconds=delay),
            owner_id,
            fence_token,
            throttle_tool=error.throttle_tool,
        )

    @staticmethod
    def _result_payload(result: ToolResult) -> dict[str, Any]:
        return result.model_dump(mode="json")
