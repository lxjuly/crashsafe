from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from crashsafe.history import (
    ATTEMPT_EVENT_SCHEMA_VERSION,
    EVENT_SCHEMA_VERSION,
    canonical_payload,
    reduce_workflow_history,
)
from crashsafe.models import (
    ClaimedStep,
    EventPayload,
    EventType,
    StepAttemptStartedPayload,
    StepCompletedPayload,
    StepDefinition,
    StepFailedPayload,
    StepName,
    StepRecord,
    StepRetryScheduledPayload,
    StepStatus,
    ToolThrottle,
    WorkflowAudit,
    WorkflowCompletedPayload,
    WorkflowCreate,
    WorkflowCreatedPayload,
    WorkflowEventRecord,
    WorkflowFailedPayload,
    WorkflowLease,
    WorkflowRecord,
    WorkflowStatus,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_db(value: datetime) -> str:
    return value.isoformat()


class WorkflowNotFoundError(KeyError):
    pass


class LegacyDatabaseError(RuntimeError):
    pass


class LeaseLostError(RuntimeError):
    pass


class SQLiteStorage:
    """Synchronous SQLite persistence with explicit, short transactions."""

    def __init__(self, path: Path, default_tool_key: str = "mock-tool") -> None:
        self.path = path
        self.default_tool_key = default_tool_key
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
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
                CREATE TABLE IF NOT EXISTS workflows (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS steps (
                    id TEXT PRIMARY KEY,
                    workflow_id TEXT NOT NULL REFERENCES workflows(id),
                    position INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    operation_key TEXT NOT NULL UNIQUE,
                    tool_key TEXT NOT NULL DEFAULT 'mock-tool',
                    output_json TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    UNIQUE(workflow_id, position)
                );

                CREATE INDEX IF NOT EXISTS idx_steps_runnable
                ON steps(status, next_attempt_at, workflow_id, position);

                CREATE TABLE IF NOT EXISTS workflow_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workflow_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    step_id TEXT,
                    attempt INTEGER,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    UNIQUE(workflow_id, sequence)
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_events_order
                ON workflow_events(workflow_id, sequence);

                CREATE TABLE IF NOT EXISTS workflow_leases (
                    workflow_id TEXT PRIMARY KEY REFERENCES workflows(id),
                    owner_id TEXT NOT NULL,
                    fence_token INTEGER NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_leases_expiry
                ON workflow_leases(lease_expires_at);

                CREATE TABLE IF NOT EXISTS tool_throttles (
                    tool_key TEXT PRIMARY KEY,
                    blocked_until TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TRIGGER IF NOT EXISTS workflow_events_no_update
                BEFORE UPDATE ON workflow_events
                BEGIN
                    SELECT RAISE(ABORT, 'workflow events are append-only');
                END;

                CREATE TRIGGER IF NOT EXISTS workflow_events_no_delete
                BEFORE DELETE ON workflow_events
                BEGIN
                    SELECT RAISE(ABORT, 'workflow events are append-only');
                END;
                """
            )
            step_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(steps)").fetchall()
            }
            if "tool_key" not in step_columns:
                connection.execute(
                    "ALTER TABLE steps ADD COLUMN tool_key TEXT NOT NULL DEFAULT 'mock-tool'"
                )
            connection.commit()
            legacy_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM workflows w
                WHERE NOT EXISTS (
                    SELECT 1 FROM workflow_events e
                    WHERE e.workflow_id = w.id AND e.event_type = ?
                )
                """,
                (EventType.WORKFLOW_CREATED.value,),
            ).fetchone()[0]
            if legacy_count:
                raise LegacyDatabaseError(
                    "database contains workflows without event history; "
                    "back it up and run make clean"
                )
        finally:
            connection.close()

    def create_workflow(
        self, workflow_input: WorkflowCreate, tool_key: Optional[str] = None
    ) -> WorkflowRecord:
        workflow_id = str(uuid.uuid4())
        now = utc_now()
        step_specs: Sequence[tuple[StepName, dict[str, Any]]] = (
            (
                StepName.CHARGE,
                {
                    "customer_id": workflow_input.customer_id,
                    "amount_cents": workflow_input.amount_cents,
                },
            ),
            (
                StepName.PROVISION,
                {"customer_id": workflow_input.customer_id, "plan": "standard"},
            ),
            (
                StepName.NOTIFY,
                {
                    "customer_id": workflow_input.customer_id,
                    "email": workflow_input.email,
                    "message": "Your account is ready.",
                },
            ),
        )
        definitions = [
            StepDefinition(
                id=str(uuid.uuid4()),
                position=position,
                name=name,
                request=request,
                operation_key=f"{workflow_id}:{position}:{name.value}",
                tool_key=tool_key or self.default_tool_key,
            )
            for position, (name, request) in enumerate(step_specs)
        ]
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO workflows VALUES (?, ?, ?, ?, ?, NULL)",
                (
                    workflow_id,
                    WorkflowStatus.RUNNING.value,
                    workflow_input.model_dump_json(),
                    to_db(now),
                    to_db(now),
                ),
            )
            for definition in definitions:
                connection.execute(
                    """
                    INSERT INTO steps (
                        id, workflow_id, position, name, status, request_json,
                        operation_key, tool_key, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        definition.id,
                        workflow_id,
                        definition.position,
                        definition.name.value,
                        StepStatus.PENDING.value,
                        self._json_dump(definition.request),
                        definition.operation_key,
                        definition.tool_key,
                        to_db(now),
                        to_db(now),
                    ),
                )
            self._append_event(
                connection,
                workflow_id,
                EventType.WORKFLOW_CREATED,
                WorkflowCreatedPayload(input=workflow_input, steps=definitions),
                now,
            )
        return self.get_workflow(workflow_id)

    def get_workflow(self, workflow_id: str) -> WorkflowRecord:
        connection = self._connect()
        try:
            workflow = connection.execute(
                "SELECT * FROM workflows WHERE id = ?", (workflow_id,)
            ).fetchone()
            if workflow is None:
                raise WorkflowNotFoundError(workflow_id)
            steps = connection.execute(
                "SELECT * FROM steps WHERE workflow_id = ? ORDER BY position", (workflow_id,)
            ).fetchall()
            return self._workflow_from_rows(workflow, steps)
        finally:
            connection.close()

    def list_workflows(self, limit: int = 100) -> list[WorkflowRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT id FROM workflows ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        finally:
            connection.close()
        return [self.get_workflow(str(row["id"])) for row in rows]

    def list_events(self, workflow_id: str) -> list[WorkflowEventRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM workflow_events
                WHERE workflow_id = ?
                ORDER BY sequence
                """,
                (workflow_id,),
            ).fetchall()
            if not rows:
                raise WorkflowNotFoundError(workflow_id)
            return [self._event_from_row(row) for row in rows]
        finally:
            connection.close()

    def audit_workflow(self, workflow_id: str) -> WorkflowAudit:
        projected = self.get_workflow(workflow_id)
        rebuilt = reduce_workflow_history(self.list_events(workflow_id))
        return WorkflowAudit(
            consistent=projected == rebuilt,
            projected=projected,
            rebuilt=rebuilt,
        )

    def rebuild_projection(self, workflow_id: str) -> WorkflowRecord:
        rebuilt = reduce_workflow_history(self.list_events(workflow_id))
        with self._transaction() as connection:
            connection.execute("DELETE FROM workflow_leases WHERE workflow_id = ?", (workflow_id,))
            connection.execute("DELETE FROM steps WHERE workflow_id = ?", (workflow_id,))
            connection.execute("DELETE FROM workflows WHERE id = ?", (workflow_id,))
            connection.execute(
                """
                INSERT INTO workflows (
                    id, status, input_json, created_at, updated_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    rebuilt.id,
                    rebuilt.status.value,
                    rebuilt.input.model_dump_json(),
                    to_db(rebuilt.created_at),
                    to_db(rebuilt.updated_at),
                    None if rebuilt.completed_at is None else to_db(rebuilt.completed_at),
                ),
            )
            for step in rebuilt.steps:
                connection.execute(
                    """
                    INSERT INTO steps (
                        id, workflow_id, position, name, status, request_json,
                        operation_key, tool_key, output_json, attempts, next_attempt_at,
                        last_error, created_at, updated_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        step.id,
                        step.workflow_id,
                        step.position,
                        step.name.value,
                        step.status.value,
                        self._json_dump(step.request),
                        step.operation_key,
                        step.tool_key,
                        None if step.output is None else self._json_dump(step.output),
                        step.attempts,
                        (
                            None
                            if step.next_attempt_at is None
                            else to_db(step.next_attempt_at)
                        ),
                        step.last_error,
                        to_db(step.created_at),
                        to_db(step.updated_at),
                        None if step.completed_at is None else to_db(step.completed_at),
                    ),
                )
        return self.get_workflow(workflow_id)

    def next_runnable_step(self, now: Optional[datetime] = None) -> Optional[StepRecord]:
        current = to_db(now or utc_now())
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT s.*
                FROM steps s
                JOIN workflows w ON w.id = s.workflow_id
                WHERE w.status = ?
                  AND s.status IN (?, ?, ?)
                  AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM steps previous
                      WHERE previous.workflow_id = s.workflow_id
                        AND previous.position < s.position
                        AND previous.status != ?
                  )
                ORDER BY w.created_at, s.position
                LIMIT 1
                """,
                (
                    WorkflowStatus.RUNNING.value,
                    StepStatus.PENDING.value,
                    StepStatus.INTENT_RECORDED.value,
                    StepStatus.RETRY_WAIT.value,
                    current,
                    StepStatus.COMPLETED.value,
                ),
            ).fetchone()
            return None if row is None else self._step_from_row(row)
        finally:
            connection.close()

    def claim_runnable_step(
        self,
        owner_id: str,
        lease_ttl_seconds: float,
        now: Optional[datetime] = None,
    ) -> Optional[ClaimedStep]:
        """Atomically select a runnable workflow and acquire its fenced lease."""
        current_time = now or utc_now()
        current = to_db(current_time)
        expires = to_db(current_time + timedelta(seconds=lease_ttl_seconds))
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT s.*
                FROM steps s
                JOIN workflows w ON w.id = s.workflow_id
                LEFT JOIN workflow_leases l ON l.workflow_id = s.workflow_id
                LEFT JOIN tool_throttles t ON t.tool_key = s.tool_key
                WHERE w.status = ?
                  AND s.status IN (?, ?, ?)
                  AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= ?)
                  AND (t.blocked_until IS NULL OR t.blocked_until <= ?)
                  AND (l.workflow_id IS NULL OR l.lease_expires_at <= ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM steps previous
                      WHERE previous.workflow_id = s.workflow_id
                        AND previous.position < s.position
                        AND previous.status != ?
                  )
                ORDER BY w.created_at, s.position
                LIMIT 1
                """,
                (
                    WorkflowStatus.RUNNING.value,
                    StepStatus.PENDING.value,
                    StepStatus.INTENT_RECORDED.value,
                    StepStatus.RETRY_WAIT.value,
                    current,
                    current,
                    current,
                    StepStatus.COMPLETED.value,
                ),
            ).fetchone()
            if row is None:
                return None
            workflow_id = str(row["workflow_id"])
            existing = connection.execute(
                "SELECT * FROM workflow_leases WHERE workflow_id = ?", (workflow_id,)
            ).fetchone()
            if existing is None:
                fence_token = 1
                connection.execute(
                    """
                    INSERT INTO workflow_leases (
                        workflow_id, owner_id, fence_token, lease_expires_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (workflow_id, owner_id, fence_token, expires, current),
                )
            else:
                fence_token = int(existing["fence_token"]) + 1
                connection.execute(
                    """
                    UPDATE workflow_leases
                    SET owner_id = ?, fence_token = ?, lease_expires_at = ?, updated_at = ?
                    WHERE workflow_id = ?
                    """,
                    (owner_id, fence_token, expires, current, workflow_id),
                )
            lease = WorkflowLease.model_validate(
                {
                    "workflow_id": workflow_id,
                    "owner_id": owner_id,
                    "fence_token": fence_token,
                    "lease_expires_at": expires,
                    "updated_at": current,
                }
            )
            return ClaimedStep(step=self._step_from_row(row), lease=lease)

    def renew_lease(
        self,
        workflow_id: str,
        owner_id: str,
        fence_token: int,
        lease_ttl_seconds: float,
    ) -> bool:
        now = utc_now()
        expires = now + timedelta(seconds=lease_ttl_seconds)
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE workflow_leases
                SET lease_expires_at = ?, updated_at = ?
                WHERE workflow_id = ? AND owner_id = ? AND fence_token = ?
                  AND lease_expires_at > ?
                """,
                (to_db(expires), to_db(now), workflow_id, owner_id, fence_token, to_db(now)),
            )
            return cursor.rowcount == 1

    def release_lease(self, workflow_id: str, owner_id: str, fence_token: int) -> bool:
        now = utc_now()
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE workflow_leases
                SET lease_expires_at = ?, updated_at = ?
                WHERE workflow_id = ? AND owner_id = ? AND fence_token = ?
                """,
                (to_db(now), to_db(now), workflow_id, owner_id, fence_token),
            )
            return cursor.rowcount == 1

    def get_lease(self, workflow_id: str) -> Optional[WorkflowLease]:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM workflow_leases WHERE workflow_id = ?", (workflow_id,)
            ).fetchone()
            return None if row is None else WorkflowLease.model_validate(dict(row))
        finally:
            connection.close()

    def get_tool_throttle(self, tool_key: str) -> Optional[ToolThrottle]:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM tool_throttles WHERE tool_key = ?", (tool_key,)
            ).fetchone()
            return None if row is None else ToolThrottle.model_validate(dict(row))
        finally:
            connection.close()

    def record_attempt(
        self,
        step_id: str,
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> StepRecord:
        """Durably record intent before any external request leaves the process."""
        now = utc_now()
        with self._transaction() as connection:
            previous = connection.execute(
                "SELECT * FROM steps WHERE id = ?", (step_id,)
            ).fetchone()
            if previous is None:
                raise KeyError(step_id)
            self._require_lease(connection, previous, owner_id, fence_token)
            if previous["status"] not in {
                StepStatus.PENDING.value,
                StepStatus.INTENT_RECORDED.value,
                StepStatus.RETRY_WAIT.value,
            }:
                raise RuntimeError(f"step {step_id} is not runnable")
            attempt = int(previous["attempts"]) + 1
            self._append_event(
                connection,
                str(previous["workflow_id"]),
                EventType.STEP_ATTEMPT_STARTED,
                StepAttemptStartedPayload(
                    name=previous["name"],
                    request=json.loads(previous["request_json"]),
                    operation_key=previous["operation_key"],
                    worker_id=owner_id,
                    fence_token=fence_token,
                ),
                now,
                step_id=step_id,
                attempt=attempt,
                schema_version=(
                    ATTEMPT_EVENT_SCHEMA_VERSION if owner_id is not None else EVENT_SCHEMA_VERSION
                ),
            )
            cursor = connection.execute(
                """
                UPDATE steps
                SET status = ?, attempts = attempts + 1, next_attempt_at = NULL,
                    last_error = NULL, updated_at = ?
                WHERE id = ? AND status IN (?, ?, ?)
                """,
                (
                    StepStatus.INTENT_RECORDED.value,
                    to_db(now),
                    step_id,
                    StepStatus.PENDING.value,
                    StepStatus.INTENT_RECORDED.value,
                    StepStatus.RETRY_WAIT.value,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"step {step_id} is not runnable")
            row = connection.execute("SELECT * FROM steps WHERE id = ?", (step_id,)).fetchone()
            assert row is not None
            return self._step_from_row(row)

    def schedule_retry(
        self,
        step_id: str,
        error: str,
        next_attempt_at: datetime,
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
        throttle_tool: bool = False,
    ) -> None:
        now = utc_now()
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM steps WHERE id = ?", (step_id,)).fetchone()
            if row is None:
                raise KeyError(step_id)
            self._require_lease(connection, row, owner_id, fence_token)
            if row["status"] != StepStatus.INTENT_RECORDED.value:
                raise RuntimeError(f"step {step_id} has no active attempt")
            self._append_event(
                connection,
                str(row["workflow_id"]),
                EventType.STEP_RETRY_SCHEDULED,
                StepRetryScheduledPayload(error=error, next_attempt_at=next_attempt_at),
                now,
                step_id=step_id,
                attempt=int(row["attempts"]),
            )
            cursor = connection.execute(
                """
                UPDATE steps SET status = ?, last_error = ?, next_attempt_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    StepStatus.RETRY_WAIT.value,
                    error,
                    to_db(next_attempt_at),
                    to_db(now),
                    step_id,
                    StepStatus.INTENT_RECORDED.value,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"step {step_id} retry transition failed")
            if throttle_tool:
                connection.execute(
                    """
                    INSERT INTO tool_throttles (tool_key, blocked_until, reason, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(tool_key) DO UPDATE SET
                        blocked_until = MAX(tool_throttles.blocked_until, excluded.blocked_until),
                        reason = CASE
                            WHEN excluded.blocked_until >= tool_throttles.blocked_until
                            THEN excluded.reason ELSE tool_throttles.reason END,
                        updated_at = excluded.updated_at
                    """,
                    (row["tool_key"], to_db(next_attempt_at), error, to_db(now)),
                )

    def fail_step(
        self,
        step_id: str,
        error: str,
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> None:
        now = utc_now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM steps WHERE id = ?", (step_id,)
            ).fetchone()
            if row is None:
                raise KeyError(step_id)
            self._require_lease(connection, row, owner_id, fence_token)
            if row["status"] != StepStatus.INTENT_RECORDED.value:
                raise RuntimeError(f"step {step_id} has no active attempt")
            workflow_id = str(row["workflow_id"])
            attempt = int(row["attempts"])
            self._append_event(
                connection,
                workflow_id,
                EventType.STEP_FAILED,
                StepFailedPayload(error=error),
                now,
                step_id=step_id,
                attempt=attempt,
            )
            self._append_event(
                connection,
                workflow_id,
                EventType.WORKFLOW_FAILED,
                WorkflowFailedPayload(step_id=step_id, error=error),
                now,
                step_id=step_id,
            )
            connection.execute(
                """
                UPDATE steps SET status = ?, last_error = ?, updated_at = ? WHERE id = ?
                """,
                (StepStatus.FAILED.value, error, to_db(now), step_id),
            )
            connection.execute(
                "UPDATE workflows SET status = ?, updated_at = ? WHERE id = ?",
                (WorkflowStatus.FAILED.value, to_db(now), workflow_id),
            )

    def complete_step(
        self,
        step_id: str,
        output: dict[str, Any],
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> None:
        """Atomically commit the step output and, if final, workflow completion."""
        now = utc_now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM steps WHERE id = ?", (step_id,)
            ).fetchone()
            if row is None:
                raise KeyError(step_id)
            self._require_lease(connection, row, owner_id, fence_token)
            if row["status"] != StepStatus.INTENT_RECORDED.value:
                return
            workflow_id = str(row["workflow_id"])
            attempt = int(row["attempts"])
            self._append_event(
                connection,
                workflow_id,
                EventType.STEP_COMPLETED,
                StepCompletedPayload(output=output),
                now,
                step_id=step_id,
                attempt=attempt,
            )
            cursor = connection.execute(
                """
                UPDATE steps
                SET status = ?, output_json = ?, next_attempt_at = NULL,
                    last_error = NULL, completed_at = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    StepStatus.COMPLETED.value,
                    self._json_dump(output),
                    to_db(now),
                    to_db(now),
                    step_id,
                    StepStatus.INTENT_RECORDED.value,
                ),
            )
            if cursor.rowcount != 1:
                return
            remaining = connection.execute(
                "SELECT COUNT(*) FROM steps WHERE workflow_id = ? AND status != ?",
                (workflow_id, StepStatus.COMPLETED.value),
            ).fetchone()[0]
            if remaining == 0:
                self._append_event(
                    connection,
                    workflow_id,
                    EventType.WORKFLOW_COMPLETED,
                    WorkflowCompletedPayload(),
                    now,
                )
                connection.execute(
                    """
                    UPDATE workflows SET status = ?, completed_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        WorkflowStatus.COMPLETED.value,
                        to_db(now),
                        to_db(now),
                        workflow_id,
                    ),
                )

    def _append_event(
        self,
        connection: sqlite3.Connection,
        workflow_id: str,
        event_type: EventType,
        payload: EventPayload,
        occurred_at: datetime,
        *,
        step_id: Optional[str] = None,
        attempt: Optional[int] = None,
        schema_version: int = EVENT_SCHEMA_VERSION,
    ) -> None:
        payload_data = canonical_payload(event_type, payload)
        sequence = int(
            connection.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1
                FROM workflow_events WHERE workflow_id = ?
                """,
                (workflow_id,),
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO workflow_events (
                workflow_id, sequence, event_type, schema_version, step_id,
                attempt, payload_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workflow_id,
                sequence,
                event_type.value,
                schema_version,
                step_id,
                attempt,
                self._json_dump(payload_data),
                to_db(occurred_at),
            ),
        )
        self._debug_pause_after_event(event_type)

    @staticmethod
    def _debug_pause_after_event(event_type: EventType) -> None:
        if os.getenv("CRASHSAFE_PAUSE_AFTER_EVENT") != event_type.value:
            return
        signal_file = os.getenv("CRASHSAFE_EVENT_SIGNAL_FILE")
        if signal_file:
            path = Path(signal_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(event_type.value, encoding="utf-8")
        time.sleep(float(os.getenv("CRASHSAFE_EVENT_PAUSE_SECONDS", "30")))

    @staticmethod
    def _require_lease(
        connection: sqlite3.Connection,
        step: sqlite3.Row,
        owner_id: Optional[str],
        fence_token: Optional[int],
    ) -> None:
        if owner_id is None and fence_token is None:
            return
        if owner_id is None or fence_token is None:
            raise ValueError("owner_id and fence_token must be supplied together")
        now = to_db(utc_now())
        lease = connection.execute(
            """
            SELECT 1 FROM workflow_leases
            WHERE workflow_id = ? AND owner_id = ? AND fence_token = ?
              AND lease_expires_at > ?
            """,
            (step["workflow_id"], owner_id, fence_token, now),
        ).fetchone()
        if lease is None:
            raise LeaseLostError(
                f"worker {owner_id} no longer owns workflow {step['workflow_id']}"
            )

    @staticmethod
    def _json_dump(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> WorkflowEventRecord:
        return WorkflowEventRecord(
            id=row["id"],
            workflow_id=row["workflow_id"],
            sequence=row["sequence"],
            event_type=row["event_type"],
            schema_version=row["schema_version"],
            step_id=row["step_id"],
            attempt=row["attempt"],
            payload=json.loads(row["payload_json"]),
            occurred_at=row["occurred_at"],
        )

    @staticmethod
    def _step_from_row(row: sqlite3.Row) -> StepRecord:
        return StepRecord(
            id=row["id"],
            workflow_id=row["workflow_id"],
            position=row["position"],
            name=row["name"],
            status=row["status"],
            request=json.loads(row["request_json"]),
            operation_key=row["operation_key"],
            tool_key=row["tool_key"],
            output=None if row["output_json"] is None else json.loads(row["output_json"]),
            attempts=row["attempts"],
            next_attempt_at=row["next_attempt_at"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )

    def _workflow_from_rows(
        self, workflow: sqlite3.Row, steps: Sequence[sqlite3.Row]
    ) -> WorkflowRecord:
        return WorkflowRecord(
            id=workflow["id"],
            status=workflow["status"],
            input=WorkflowCreate.model_validate_json(workflow["input_json"]),
            steps=[self._step_from_row(step) for step in steps],
            created_at=workflow["created_at"],
            updated_at=workflow["updated_at"],
            completed_at=workflow["completed_at"],
        )
