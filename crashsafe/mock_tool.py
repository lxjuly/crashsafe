from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel

from crashsafe.config import (
    DEBUG_COMMIT_DELAY_SECONDS,
    DEFAULT_API_HOST,
    DEFAULT_FLAKY_RATE,
    DEFAULT_RETRY_AFTER_SECONDS,
    DEFAULT_TOOL_PORT,
    Settings,
)
from crashsafe.engine import DEBUG_DELAY_HEADER, IDEMPOTENCY_HEADER, RETRY_AFTER_HEADER
from crashsafe.models import (
    ChargeRequest,
    LedgerSummary,
    NotifyRequest,
    ProvisionRequest,
    StepName,
    ToolResult,
)
from crashsafe.storage import to_db, utc_now


class IdempotencyConflictError(Exception):
    pass


class ToolStore:
    """Separate durable service: side effect and idempotency result share one transaction."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS idempotency_results (
                    operation_key TEXT PRIMARY KEY,
                    operation TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS side_effects (
                    id TEXT PRIMARY KEY,
                    operation_key TEXT NOT NULL UNIQUE,
                    operation TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            connection.commit()
        finally:
            connection.close()

    def apply(
        self, operation: StepName, operation_key: str, request: BaseModel
    ) -> tuple[ToolResult, bool]:
        canonical_request = json.dumps(request.model_dump(mode="json"), sort_keys=True)
        request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM idempotency_results WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["operation"] != operation.value
                    or existing["request_hash"] != request_hash
                ):
                    raise IdempotencyConflictError(operation_key)
                saved = ToolResult.model_validate_json(existing["response_json"])
                return saved.model_copy(update={"deduplicated": True}), False

            reference_id = f"{operation.value}_{uuid.uuid4().hex[:16]}"
            result = ToolResult(operation=operation, reference_id=reference_id)
            now = to_db(utc_now())
            connection.execute(
                "INSERT INTO side_effects VALUES (?, ?, ?, ?, ?)",
                (reference_id, operation_key, operation.value, canonical_request, now),
            )
            connection.execute(
                "INSERT INTO idempotency_results VALUES (?, ?, ?, ?, ?)",
                (operation_key, operation.value, request_hash, result.model_dump_json(), now),
            )
            return result, True

    def find(
        self, operation: StepName, operation_key: str, request: BaseModel
    ) -> Optional[ToolResult]:
        canonical_request = json.dumps(request.model_dump(mode="json"), sort_keys=True)
        request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM idempotency_results WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if row is None:
                return None
            if row["operation"] != operation.value or row["request_hash"] != request_hash:
                raise IdempotencyConflictError(operation_key)
            saved = ToolResult.model_validate_json(row["response_json"])
            return saved.model_copy(update={"deduplicated": True})
        finally:
            connection.close()

    def summary(self) -> LedgerSummary:
        connection = self._connect()
        try:
            counts = {
                row["operation"]: row["count"]
                for row in connection.execute(
                    "SELECT operation, COUNT(*) AS count FROM side_effects GROUP BY operation"
                )
            }
            return LedgerSummary(
                charges=counts.get(StepName.CHARGE.value, 0),
                provisions=counts.get(StepName.PROVISION.value, 0),
                notifications=counts.get(StepName.NOTIFY.value, 0),
            )
        finally:
            connection.close()


def create_app(store: Optional[ToolStore] = None) -> FastAPI:
    settings = Settings.from_env()
    tool_store = store or ToolStore(settings.tool_db)
    app = FastAPI(title="Crashsafe flaky mock tool", version="0.1.0")
    flaky_rate = float(os.getenv("CRASHSAFE_FLAKY_RATE", str(DEFAULT_FLAKY_RATE)))
    retry_after = float(os.getenv("CRASHSAFE_RETRY_AFTER", str(DEFAULT_RETRY_AFTER_SECONDS)))
    forced_failures = int(os.getenv("CRASHSAFE_FAIL_FIRST_N", "0"))
    forced_failure_operation_value = os.getenv("CRASHSAFE_FAIL_FIRST_OPERATION")
    forced_failure_operation = (
        None if forced_failure_operation_value is None else StepName(forced_failure_operation_value)
    )
    operation_failure_pending = forced_failure_operation is not None
    failure_lock = threading.Lock()
    rng = random.Random(os.getenv("CRASHSAFE_RANDOM_SEED"))
    request_types: dict[StepName, type[BaseModel]] = {
        StepName.CHARGE: ChargeRequest,
        StepName.PROVISION: ProvisionRequest,
        StepName.NOTIFY: NotifyRequest,
    }

    def execute(
        operation: StepName,
        payload: dict[str, Any],
        operation_key: str,
        delay_after_commit: Optional[str],
    ) -> ToolResult:
        nonlocal forced_failures, operation_failure_pending
        request = request_types[operation].model_validate(payload)
        try:
            committed = tool_store.find(operation, operation_key, request)
        except IdempotencyConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="idempotency key conflicts with an earlier request",
            ) from exc
        if committed is not None:
            return committed

        # Flakiness occurs before the side effect transaction.
        with failure_lock:
            force_failure = forced_failures > 0
            if force_failure:
                forced_failures -= 1
            elif operation_failure_pending and operation == forced_failure_operation:
                operation_failure_pending = False
                force_failure = True
        if force_failure or rng.random() < flaky_rate:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="mock tool is temporarily throttled",
                headers={RETRY_AFTER_HEADER: str(retry_after)},
            )
        try:
            result, created = tool_store.apply(operation, operation_key, request)
        except IdempotencyConflictError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="idempotency key conflicts with an earlier request",
            ) from exc

        # The side effect and result have committed. Sleeping creates the ambiguous window.
        if created:
            signal_file = os.getenv("CRASHSAFE_COMMIT_SIGNAL_FILE")
            if signal_file:
                Path(signal_file).write_text(operation_key, encoding="utf-8")
            if delay_after_commit == "true":
                time.sleep(float(os.getenv("CRASHSAFE_COMMIT_DELAY", DEBUG_COMMIT_DELAY_SECONDS)))
        return result

    @app.post("/tools/{operation}", response_model=ToolResult)
    def invoke_tool(
        operation: StepName,
        payload: dict[str, Any],
        idempotency_key: str = Header(alias=IDEMPOTENCY_HEADER),
        delay_after_commit: Optional[str] = Header(default=None, alias=DEBUG_DELAY_HEADER),
    ) -> ToolResult:
        return execute(operation, payload, idempotency_key, delay_after_commit)

    @app.get("/ledger", response_model=LedgerSummary)
    def ledger() -> LedgerSummary:
        return tool_store.summary()

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def run() -> None:
    port = int(os.getenv("CRASHSAFE_TOOL_PORT", str(DEFAULT_TOOL_PORT)))
    uvicorn.run(create_app(), host=DEFAULT_API_HOST, port=port, log_level="info")


if __name__ == "__main__":
    run()
