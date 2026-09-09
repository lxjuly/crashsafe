"""SQLite persistence for event history, projections, leases, and retry timing.

Every logical transition appends its event and updates scheduling projections in
one transaction. The append-only history remains capable of rebuilding those rows.
"""

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
from typing import Any, Optional, cast

from crashsafe.history import (
    ATTEMPT_EVENT_SCHEMA_VERSION,
    EVENT_SCHEMA_VERSION,
    WORKFLOW_RUN_CREATED_SCHEMA_VERSION,
    canonical_payload,
    reduce_workflow_run_history,
)
from crashsafe.models import (
    ClaimedStep,
    EventPayload,
    EventType,
    StepAttemptStartedPayload,
    StepCompletedPayload,
    StepDefinition,
    StepFailedPayload,
    StepRecord,
    StepRetryScheduledPayload,
    StepStatus,
    ToolThrottle,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowRunCompletedPayload,
    WorkflowRunCreatedPayload,
    WorkflowRunEvent,
    WorkflowRunFailedPayload,
    WorkflowRunLease,
    WorkflowRunStatus,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_db(value: datetime) -> str:
    return value.isoformat()


class WorkflowRunNotFoundError(KeyError):
    pass


class LegacyDatabaseError(RuntimeError):
    pass


class LeaseLostError(RuntimeError):
    pass


class SQLiteStorage:
    """SQLite event history plus an atomically maintained scheduling projection.

    Public mutation methods each define a complete workflow transition boundary;
    callers never receive a connection or compose transactions across network I/O.
    """

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
        # FULL asks SQLite to sync each commit through the OS before reporting
        # success, which is the durability boundary used by the engine.
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            # IMMEDIATE obtains the single-writer reservation up front. Claims,
            # events, and their projections therefore cannot interleave halfway.
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
            # SQLite's WAL protects each database commit from process failure;
            # workflow_run_events is the higher-level log that survives workers.
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            legacy = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'workflows'"
            ).fetchone()
            if legacy is not None:
                columns = {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(workflows)").fetchall()
                }
                if "definition_json" not in columns:
                    count = int(connection.execute("SELECT COUNT(*) FROM workflows").fetchone()[0])
                    if count:
                        raise LegacyDatabaseError(
                            "database uses the pre-DAG schema; back it up and remove .crashsafe/"
                        )
                    self._drop_empty_legacy_schema(connection)
                else:
                    self._migrate_v3_to_v4(connection)
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS steps (
                    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id),
                    id TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    operation TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    operation_key TEXT NOT NULL UNIQUE,
                    tool_key TEXT NOT NULL,
                    output_json TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    PRIMARY KEY(run_id, id),
                    UNIQUE(run_id, position)
                );

                CREATE TABLE IF NOT EXISTS step_dependencies (
                    run_id TEXT NOT NULL,
                    step_id TEXT NOT NULL,
                    dependency_step_id TEXT NOT NULL,
                    PRIMARY KEY(run_id, step_id, dependency_step_id),
                    FOREIGN KEY(run_id, step_id) REFERENCES steps(run_id, id),
                    FOREIGN KEY(run_id, dependency_step_id) REFERENCES steps(run_id, id)
                );

                CREATE INDEX IF NOT EXISTS idx_steps_runnable
                ON steps(status, next_attempt_at, run_id, position);

                CREATE TABLE IF NOT EXISTS workflow_run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    step_id TEXT,
                    attempt INTEGER,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    UNIQUE(run_id, sequence)
                );

                CREATE INDEX IF NOT EXISTS idx_workflow_run_events_order
                ON workflow_run_events(run_id, sequence);

                CREATE TABLE IF NOT EXISTS workflow_run_leases (
                    run_id TEXT PRIMARY KEY REFERENCES workflow_runs(run_id),
                    owner_id TEXT NOT NULL,
                    fence_token INTEGER NOT NULL,
                    lease_expires_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tool_throttles (
                    tool_key TEXT PRIMARY KEY,
                    blocked_until TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TRIGGER IF NOT EXISTS workflow_run_events_no_update
                BEFORE UPDATE ON workflow_run_events BEGIN
                    SELECT RAISE(ABORT, 'workflow run events are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS workflow_run_events_no_delete
                BEFORE DELETE ON workflow_run_events BEGIN
                    SELECT RAISE(ABORT, 'workflow run events are append-only');
                END;
                PRAGMA user_version = 4;
                """
            )
            connection.commit()
        finally:
            connection.close()

    @staticmethod
    def _drop_empty_legacy_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            DROP TRIGGER IF EXISTS workflow_events_no_update;
            DROP TRIGGER IF EXISTS workflow_events_no_delete;
            DROP TRIGGER IF EXISTS workflow_run_events_no_update;
            DROP TRIGGER IF EXISTS workflow_run_events_no_delete;
            DROP TABLE IF EXISTS step_dependencies;
            DROP TABLE IF EXISTS workflow_leases;
            DROP TABLE IF EXISTS workflow_run_leases;
            DROP TABLE IF EXISTS tool_throttles;
            DROP TABLE IF EXISTS steps;
            DROP TABLE IF EXISTS workflows;
            DROP TABLE IF EXISTS workflow_runs;
            DROP TABLE IF EXISTS workflow_events;
            DROP TABLE IF EXISTS workflow_run_events;
            """
        )

    @staticmethod
    def _migrate_v3_to_v4(connection: sqlite3.Connection) -> None:
        """Rename execution state without discarding existing durable runs."""
        connection.execute("PRAGMA foreign_keys = OFF")
        try:
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                DROP TRIGGER IF EXISTS workflow_events_no_update;
                DROP TRIGGER IF EXISTS workflow_events_no_delete;
                DROP INDEX IF EXISTS idx_steps_runnable;
                DROP INDEX IF EXISTS idx_workflow_events_order;

                ALTER TABLE workflows RENAME TO workflow_runs;
                ALTER TABLE workflow_runs RENAME COLUMN id TO run_id;
                ALTER TABLE steps RENAME COLUMN workflow_id TO run_id;
                ALTER TABLE step_dependencies RENAME COLUMN workflow_id TO run_id;
                ALTER TABLE workflow_events RENAME TO workflow_run_events;
                ALTER TABLE workflow_run_events RENAME COLUMN workflow_id TO run_id;
                ALTER TABLE workflow_leases RENAME TO workflow_run_leases;
                ALTER TABLE workflow_run_leases RENAME COLUMN workflow_id TO run_id;

                UPDATE workflow_run_events
                SET event_type = CASE event_type
                    WHEN 'WorkflowCreated' THEN 'WorkflowRunCreated'
                    WHEN 'WorkflowCompleted' THEN 'WorkflowRunCompleted'
                    WHEN 'WorkflowFailed' THEN 'WorkflowRunFailed'
                    ELSE event_type
                END;

                PRAGMA user_version = 4;
                COMMIT;
                """
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.execute("PRAGMA foreign_keys = ON")

    def create_workflow_run(
        self, definition: WorkflowDefinition, tool_key: Optional[str] = None
    ) -> WorkflowRun:
        run_id = str(uuid.uuid4())
        now = utc_now()
        selected_tool = tool_key or self.default_tool_key
        definitions = [
            StepDefinition(
                id=step.id,
                position=position,
                operation=step.operation,
                depends_on=list(step.depends_on),
                request=step.request.model_dump(mode="json"),
                operation_key=f"{run_id}:{step.id}",
                tool_key=selected_tool,
            )
            for position, step in enumerate(definition.steps)
        ]
        created_payload = WorkflowRunCreatedPayload(name=definition.name, steps=definitions)
        # The immutable definition snapshot and mutable scheduling projection are
        # born in the same commit, so no partially materialized run can be visible.
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO workflow_runs (
                    run_id, name, status, definition_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    definition.name,
                    WorkflowRunStatus.RUNNING.value,
                    self._json_dump(created_payload.model_dump(mode="json")),
                    to_db(now),
                    to_db(now),
                ),
            )
            for step_definition in definitions:
                connection.execute(
                    """
                    INSERT INTO steps (
                        run_id, id, position, operation, status, request_json,
                        operation_key, tool_key, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        step_definition.id,
                        step_definition.position,
                        step_definition.operation.value,
                        StepStatus.PENDING.value,
                        self._json_dump(step_definition.request),
                        step_definition.operation_key,
                        step_definition.tool_key,
                        to_db(now),
                        to_db(now),
                    ),
                )
            for step_definition in definitions:
                for dependency in step_definition.depends_on:
                    connection.execute(
                        "INSERT INTO step_dependencies VALUES (?, ?, ?)",
                        (run_id, step_definition.id, dependency),
                    )
            self._append_event(
                connection,
                run_id,
                EventType.WORKFLOW_RUN_CREATED,
                created_payload,
                now,
                schema_version=WORKFLOW_RUN_CREATED_SCHEMA_VERSION,
            )
        return self.get_workflow_run(run_id)

    def get_workflow_run(self, run_id: str) -> WorkflowRun:
        connection = self._connect()
        try:
            run = connection.execute(
                "SELECT * FROM workflow_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run is None:
                raise WorkflowRunNotFoundError(run_id)
            steps = connection.execute(
                "SELECT * FROM steps WHERE run_id = ? ORDER BY position", (run_id,)
            ).fetchall()
            dependencies = self._dependencies(connection, run_id)
            return self._workflow_run_from_rows(run, steps, dependencies)
        finally:
            connection.close()

    def list_workflow_runs(self, limit: int = 100) -> list[WorkflowRun]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT run_id FROM workflow_runs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        finally:
            connection.close()
        return [self.get_workflow_run(str(row["run_id"])) for row in rows]

    def list_events(self, run_id: str) -> list[WorkflowRunEvent]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM workflow_run_events WHERE run_id = ? ORDER BY sequence",
                (run_id,),
            ).fetchall()
            if not rows:
                raise WorkflowRunNotFoundError(run_id)
            return [self._event_from_row(row) for row in rows]
        finally:
            connection.close()

    def projection_matches_history(self, run_id: str) -> bool:
        # The projection is a disposable read/scheduling index; history is the
        # logical source of truth and must reduce to the same public run state.
        return self.get_workflow_run(run_id) == reduce_workflow_run_history(
            self.list_events(run_id)
        )

    def rebuild_projection(self, run_id: str) -> WorkflowRun:
        # Rebuild only projection tables. The append-only events are deliberately
        # left untouched and replayed through the strict reducer first.
        rebuilt = reduce_workflow_run_history(self.list_events(run_id))
        created = WorkflowRunCreatedPayload.model_validate(self.list_events(run_id)[0].payload)
        with self._transaction() as connection:
            connection.execute("DELETE FROM workflow_run_leases WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM step_dependencies WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM steps WHERE run_id = ?", (run_id,))
            connection.execute("DELETE FROM workflow_runs WHERE run_id = ?", (run_id,))
            connection.execute(
                "INSERT INTO workflow_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    rebuilt.run_id,
                    rebuilt.name,
                    rebuilt.status.value,
                    self._json_dump(created.model_dump(mode="json")),
                    to_db(rebuilt.created_at),
                    to_db(rebuilt.updated_at),
                    None if rebuilt.completed_at is None else to_db(rebuilt.completed_at),
                ),
            )
            for step in rebuilt.steps:
                self._insert_rebuilt_step(connection, step)
            for step in rebuilt.steps:
                for dependency in step.depends_on:
                    connection.execute(
                        "INSERT INTO step_dependencies VALUES (?, ?, ?)",
                        (run_id, step.id, dependency),
                    )
        return self.get_workflow_run(run_id)

    def next_runnable_step(self, now: Optional[datetime] = None) -> Optional[StepRecord]:
        connection = self._connect()
        try:
            row = self._select_runnable(connection, to_db(now or utc_now()), include_lease=False)
            if row is None:
                return None
            return self._step_from_row(row, self._dependencies(connection, str(row["run_id"])))
        finally:
            connection.close()

    def claim_runnable_step(
        self,
        owner_id: str,
        lease_ttl_seconds: float,
        now: Optional[datetime] = None,
    ) -> Optional[ClaimedStep]:
        current_time = now or utc_now()
        current = to_db(current_time)
        expires = to_db(current_time + timedelta(seconds=lease_ttl_seconds))
        # Selection and lease replacement are one write transaction. A takeover
        # always advances the fence, invalidating commits from the former owner.
        with self._transaction() as connection:
            row = self._select_runnable(connection, current, include_lease=True)
            if row is None:
                return None
            run_id = str(row["run_id"])
            existing = connection.execute(
                "SELECT fence_token FROM workflow_run_leases WHERE run_id = ?", (run_id,)
            ).fetchone()
            fence_token = 1 if existing is None else int(existing["fence_token"]) + 1
            connection.execute(
                """
                INSERT INTO workflow_run_leases VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    owner_id=excluded.owner_id, fence_token=excluded.fence_token,
                    lease_expires_at=excluded.lease_expires_at, updated_at=excluded.updated_at
                """,
                (run_id, owner_id, fence_token, expires, current),
            )
            lease = WorkflowRunLease.model_validate(
                {
                    "run_id": run_id,
                    "owner_id": owner_id,
                    "fence_token": fence_token,
                    "lease_expires_at": expires,
                    "updated_at": current,
                }
            )
            step = self._step_from_row(row, self._dependencies(connection, run_id))
            return ClaimedStep(step=step, lease=lease)

    def _select_runnable(
        self, connection: sqlite3.Connection, current: str, *, include_lease: bool
    ) -> Optional[sqlite3.Row]:
        # Eligibility is entirely persisted: run/step state, retry deadline,
        # tool throttle, expired lease, and completed DAG dependencies.
        lease_clause = "AND (l.run_id IS NULL OR l.lease_expires_at <= ?)" if include_lease else ""
        throttle_join = (
            "LEFT JOIN tool_throttles t ON t.tool_key = s.tool_key" if include_lease else ""
        )
        throttle_clause = (
            "AND (t.blocked_until IS NULL OR t.blocked_until <= ?)" if include_lease else ""
        )
        lease_join = (
            "LEFT JOIN workflow_run_leases l ON l.run_id = s.run_id" if include_lease else ""
        )
        parameters: list[str] = [
            WorkflowRunStatus.RUNNING.value,
            StepStatus.PENDING.value,
            StepStatus.INTENT_RECORDED.value,
            StepStatus.RETRY_WAIT.value,
            current,
        ]
        if include_lease:
            parameters.extend([current, current])
        parameters.append(StepStatus.COMPLETED.value)
        row = connection.execute(
            f"""
            SELECT s.* FROM steps s
            JOIN workflow_runs w ON w.run_id = s.run_id
            {lease_join}
            {throttle_join}
            WHERE w.status = ?
              AND s.status IN (?, ?, ?)
              AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= ?)
              {throttle_clause}
              {lease_clause}
              AND NOT EXISTS (
                  SELECT 1 FROM step_dependencies d
                  JOIN steps dependency
                    ON dependency.run_id=d.run_id AND dependency.id=d.dependency_step_id
                  WHERE d.run_id=s.run_id AND d.step_id=s.id
                    AND dependency.status != ?
              )
            ORDER BY w.created_at, s.position LIMIT 1
            """,
            parameters,
        ).fetchone()
        return cast(Optional[sqlite3.Row], row)

    def renew_lease(
        self, run_id: str, owner_id: str, fence_token: int, lease_ttl_seconds: float
    ) -> bool:
        now = utc_now()
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE workflow_run_leases SET lease_expires_at=?, updated_at=?
                WHERE run_id=? AND owner_id=? AND fence_token=? AND lease_expires_at>?
                """,
                (
                    to_db(now + timedelta(seconds=lease_ttl_seconds)),
                    to_db(now),
                    run_id,
                    owner_id,
                    fence_token,
                    to_db(now),
                ),
            )
            return cursor.rowcount == 1

    def release_lease(self, run_id: str, owner_id: str, fence_token: int) -> bool:
        now = utc_now()
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE workflow_run_leases SET lease_expires_at=?, updated_at=?
                WHERE run_id=? AND owner_id=? AND fence_token=?
                """,
                (to_db(now), to_db(now), run_id, owner_id, fence_token),
            )
            return cursor.rowcount == 1

    def get_lease(self, run_id: str) -> Optional[WorkflowRunLease]:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM workflow_run_leases WHERE run_id=?", (run_id,)
            ).fetchone()
            return None if row is None else WorkflowRunLease.model_validate(dict(row))
        finally:
            connection.close()

    def get_tool_throttle(self, tool_key: str) -> Optional[ToolThrottle]:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM tool_throttles WHERE tool_key=?", (tool_key,)
            ).fetchone()
            return None if row is None else ToolThrottle.model_validate(dict(row))
        finally:
            connection.close()

    def record_attempt(
        self,
        run_id: str,
        step_id: str,
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> StepRecord:
        now = utc_now()
        with self._transaction() as connection:
            row = self._get_step_row(connection, run_id, step_id)
            self._require_lease(connection, row, owner_id, fence_token)
            if row["status"] not in {
                StepStatus.PENDING.value,
                StepStatus.INTENT_RECORDED.value,
                StepStatus.RETRY_WAIT.value,
            }:
                raise RuntimeError(f"step {step_id} is not runnable")
            blocked = connection.execute(
                """
                SELECT 1 FROM step_dependencies d JOIN steps dependency
                  ON dependency.run_id=d.run_id AND dependency.id=d.dependency_step_id
                WHERE d.run_id=? AND d.step_id=? AND dependency.status != ? LIMIT 1
                """,
                (run_id, step_id, StepStatus.COMPLETED.value),
            ).fetchone()
            if blocked is not None:
                raise RuntimeError(f"step {step_id} has incomplete dependencies")
            attempt = int(row["attempts"]) + 1
            # This is the engine's write-ahead intent: persist the exact request
            # and stable key before any network call can leave the worker.
            self._append_event(
                connection,
                run_id,
                EventType.STEP_ATTEMPT_STARTED,
                StepAttemptStartedPayload(
                    operation=row["operation"],
                    request=json.loads(row["request_json"]),
                    operation_key=row["operation_key"],
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
            connection.execute(
                """
                UPDATE steps SET status=?, attempts=?, next_attempt_at=NULL,
                    last_error=NULL, updated_at=? WHERE run_id=? AND id=?
                """,
                (StepStatus.INTENT_RECORDED.value, attempt, to_db(now), run_id, step_id),
            )
            updated = self._get_step_row(connection, run_id, step_id)
            return self._step_from_row(updated, self._dependencies(connection, run_id))

    def schedule_retry(
        self,
        run_id: str,
        step_id: str,
        error: str,
        next_attempt_at: datetime,
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
        throttle_tool: bool = False,
    ) -> None:
        now = utc_now()
        with self._transaction() as connection:
            row = self._get_step_row(connection, run_id, step_id)
            self._require_active_attempt(connection, row, owner_id, fence_token)
            # Event and deadline commit together, so restart cannot retry early or
            # lose the explanation for why this attempt is waiting.
            self._append_event(
                connection,
                run_id,
                EventType.STEP_RETRY_SCHEDULED,
                StepRetryScheduledPayload(error=error, next_attempt_at=next_attempt_at),
                now,
                step_id=step_id,
                attempt=int(row["attempts"]),
            )
            connection.execute(
                """
                UPDATE steps SET status=?, last_error=?, next_attempt_at=?, updated_at=?
                WHERE run_id=? AND id=?
                """,
                (
                    StepStatus.RETRY_WAIT.value,
                    error,
                    to_db(next_attempt_at),
                    to_db(now),
                    run_id,
                    step_id,
                ),
            )
            if throttle_tool:
                # A 429 deadline gates every run using this tool, and commits with
                # the failed attempt so restart cannot forget the shared cooldown.
                connection.execute(
                    """
                    INSERT INTO tool_throttles VALUES (?, ?, ?, ?)
                    ON CONFLICT(tool_key) DO UPDATE SET
                      blocked_until=MAX(tool_throttles.blocked_until, excluded.blocked_until),
                      reason=CASE WHEN excluded.blocked_until >= tool_throttles.blocked_until
                                  THEN excluded.reason ELSE tool_throttles.reason END,
                      updated_at=excluded.updated_at
                    """,
                    (row["tool_key"], to_db(next_attempt_at), error, to_db(now)),
                )

    def fail_step(
        self,
        run_id: str,
        step_id: str,
        error: str,
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> None:
        now = utc_now()
        with self._transaction() as connection:
            row = self._get_step_row(connection, run_id, step_id)
            self._require_active_attempt(connection, row, owner_id, fence_token)
            attempt = int(row["attempts"])
            # The failed step and terminal run events share the projection commit;
            # downstream DAG steps therefore never become eligible after failure.
            self._append_event(
                connection,
                run_id,
                EventType.STEP_FAILED,
                StepFailedPayload(error=error),
                now,
                step_id=step_id,
                attempt=attempt,
            )
            self._append_event(
                connection,
                run_id,
                EventType.WORKFLOW_RUN_FAILED,
                WorkflowRunFailedPayload(step_id=step_id, error=error),
                now,
                step_id=step_id,
            )
            connection.execute(
                """
                UPDATE steps SET status=?, last_error=?, updated_at=?
                WHERE run_id=? AND id=?
                """,
                (StepStatus.FAILED.value, error, to_db(now), run_id, step_id),
            )
            connection.execute(
                "UPDATE workflow_runs SET status=?, updated_at=? WHERE run_id=?",
                (WorkflowRunStatus.FAILED.value, to_db(now), run_id),
            )

    def complete_step(
        self,
        run_id: str,
        step_id: str,
        output: dict[str, Any],
        owner_id: Optional[str] = None,
        fence_token: Optional[int] = None,
    ) -> None:
        now = utc_now()
        with self._transaction() as connection:
            row = self._get_step_row(connection, run_id, step_id)
            self._require_lease(connection, row, owner_id, fence_token)
            if row["status"] != StepStatus.INTENT_RECORDED.value:
                return
            self._append_event(
                connection,
                run_id,
                EventType.STEP_COMPLETED,
                StepCompletedPayload(output=output),
                now,
                step_id=step_id,
                attempt=int(row["attempts"]),
            )
            connection.execute(
                """
                UPDATE steps SET status=?, output_json=?, next_attempt_at=NULL,
                    last_error=NULL, completed_at=?, updated_at=?
                WHERE run_id=? AND id=?
                """,
                (
                    StepStatus.COMPLETED.value,
                    self._json_dump(output),
                    to_db(now),
                    to_db(now),
                    run_id,
                    step_id,
                ),
            )
            remaining = int(
                connection.execute(
                    "SELECT COUNT(*) FROM steps WHERE run_id=? AND status != ?",
                    (run_id, StepStatus.COMPLETED.value),
                ).fetchone()[0]
            )
            if remaining == 0:
                # Final step output and terminal run state share a commit: readers
                # never see a completed run missing its last durable result.
                self._append_event(
                    connection,
                    run_id,
                    EventType.WORKFLOW_RUN_COMPLETED,
                    WorkflowRunCompletedPayload(),
                    now,
                )
                connection.execute(
                    """
                    UPDATE workflow_runs
                    SET status=?, completed_at=?, updated_at=? WHERE run_id=?
                    """,
                    (WorkflowRunStatus.COMPLETED.value, to_db(now), to_db(now), run_id),
                )

    def _append_event(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        event_type: EventType,
        payload: EventPayload,
        occurred_at: datetime,
        *,
        step_id: Optional[str] = None,
        attempt: Optional[int] = None,
        schema_version: int = EVENT_SCHEMA_VERSION,
    ) -> None:
        # BEGIN IMMEDIATE serializes writers, so MAX(sequence)+1 is monotonic and
        # unique for this run without a separate sequence allocator.
        sequence = int(
            connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM workflow_run_events WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO workflow_run_events (
                run_id, sequence, event_type, schema_version,
                step_id, attempt, payload_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                sequence,
                event_type.value,
                schema_version,
                step_id,
                attempt,
                self._json_dump(canonical_payload(event_type, payload)),
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
        # Every worker-originated transition rechecks the live fence inside its
        # commit transaction; a slow or partitioned former owner cannot commit.
        row = connection.execute(
            """
            SELECT 1 FROM workflow_run_leases WHERE run_id=? AND owner_id=?
              AND fence_token=? AND lease_expires_at>?
            """,
            (step["run_id"], owner_id, fence_token, to_db(utc_now())),
        ).fetchone()
        if row is None:
            raise LeaseLostError(f"worker {owner_id} no longer owns run {step['run_id']}")

    def _require_active_attempt(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        owner_id: Optional[str],
        fence_token: Optional[int],
    ) -> None:
        self._require_lease(connection, row, owner_id, fence_token)
        if row["status"] != StepStatus.INTENT_RECORDED.value:
            raise RuntimeError(f"step {row['id']} has no active attempt")

    @staticmethod
    def _get_step_row(connection: sqlite3.Connection, run_id: str, step_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM steps WHERE run_id=? AND id=?", (run_id, step_id)
        ).fetchone()
        if row is None:
            raise KeyError((run_id, step_id))
        return cast(sqlite3.Row, row)

    @staticmethod
    def _dependencies(connection: sqlite3.Connection, run_id: str) -> dict[str, list[str]]:
        values: dict[str, list[str]] = {}
        rows = connection.execute(
            """
            SELECT step_id, dependency_step_id FROM step_dependencies
            WHERE run_id=? ORDER BY rowid
            """,
            (run_id,),
        ).fetchall()
        for row in rows:
            values.setdefault(str(row["step_id"]), []).append(str(row["dependency_step_id"]))
        return values

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> WorkflowRunEvent:
        return WorkflowRunEvent(
            id=row["id"],
            run_id=row["run_id"],
            sequence=row["sequence"],
            event_type=row["event_type"],
            schema_version=row["schema_version"],
            step_id=row["step_id"],
            attempt=row["attempt"],
            payload=json.loads(row["payload_json"]),
            occurred_at=row["occurred_at"],
        )

    @staticmethod
    def _step_from_row(row: sqlite3.Row, dependencies: dict[str, list[str]]) -> StepRecord:
        return StepRecord(
            id=row["id"],
            run_id=row["run_id"],
            position=row["position"],
            operation=row["operation"],
            depends_on=dependencies.get(str(row["id"]), []),
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

    def _workflow_run_from_rows(
        self,
        run: sqlite3.Row,
        steps: Sequence[sqlite3.Row],
        dependencies: dict[str, list[str]],
    ) -> WorkflowRun:
        return WorkflowRun(
            run_id=run["run_id"],
            name=run["name"],
            status=run["status"],
            steps=[self._step_from_row(step, dependencies) for step in steps],
            created_at=run["created_at"],
            updated_at=run["updated_at"],
            completed_at=run["completed_at"],
        )

    def _insert_rebuilt_step(self, connection: sqlite3.Connection, step: StepRecord) -> None:
        connection.execute(
            """
            INSERT INTO steps (
                run_id, id, position, operation, status, request_json,
                operation_key, tool_key, output_json, attempts, next_attempt_at,
                last_error, created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                step.run_id,
                step.id,
                step.position,
                step.operation.value,
                step.status.value,
                self._json_dump(step.request),
                step.operation_key,
                step.tool_key,
                None if step.output is None else self._json_dump(step.output),
                step.attempts,
                None if step.next_attempt_at is None else to_db(step.next_attempt_at),
                step.last_error,
                to_db(step.created_at),
                to_db(step.updated_at),
                None if step.completed_at is None else to_db(step.completed_at),
            ),
        )

    @staticmethod
    def _json_dump(value: Any) -> str:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
