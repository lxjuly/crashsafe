"""Lease ownership, fencing, heartbeat, and concurrent-worker tests."""

from __future__ import annotations

import threading
import time
from dataclasses import replace
from pathlib import Path

import pytest
from workflow_fixtures import paid_workflow_definition

from crashsafe.engine import WorkflowEngine
from crashsafe.models import StepRecord, ToolResult
from crashsafe.storage import LeaseLostError, SQLiteStorage


def _create(store: SQLiteStorage, customer: str = "customer"):
    return store.create_workflow_run(paid_workflow_definition(customer))


def test_expired_owner_is_fenced_from_committing(tmp_path: Path) -> None:
    store = SQLiteStorage(tmp_path / "engine.db")
    run = _create(store)
    first = store.claim_runnable_step("worker-1", 0.05)
    assert first is not None
    attempted = store.record_attempt(
        first.step.run_id,
        first.step.id,
        first.lease.owner_id,
        first.lease.fence_token,
    )

    time.sleep(0.07)
    second = store.claim_runnable_step("worker-2", 1.0)
    assert second is not None
    assert second.lease.fence_token == first.lease.fence_token + 1

    with pytest.raises(LeaseLostError):
        store.complete_step(
            attempted.run_id,
            attempted.id,
            {"reference_id": "stale"},
            first.lease.owner_id,
            first.lease.fence_token,
        )

    retried = store.record_attempt(
        second.step.run_id,
        second.step.id,
        second.lease.owner_id,
        second.lease.fence_token,
    )
    store.complete_step(
        retried.run_id,
        retried.id,
        {"reference_id": "current"},
        second.lease.owner_id,
        second.lease.fence_token,
    )
    charge = store.get_workflow_run(run.run_id).steps[0]
    assert charge.attempts == 2
    assert charge.output == {"reference_id": "current"}


class BlockingGateway:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls: list[str] = []

    def execute(self, step: StepRecord) -> ToolResult:
        self.calls.append(step.operation_key)
        self.entered.set()
        assert self.release.wait(2)
        return ToolResult(operation=step.operation, reference_id="one")


def test_two_workers_do_not_execute_the_same_claim(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    run = _create(store)
    gateway = BlockingGateway()
    first = WorkflowEngine(store, gateway, settings, worker_id="worker-1")  # type: ignore[arg-type]
    second = WorkflowEngine(store, gateway, settings, worker_id="worker-2")  # type: ignore[arg-type]

    thread = threading.Thread(target=first.run_once)
    thread.start()
    assert gateway.entered.wait(2)
    assert second.run_once().did_work is False
    gateway.release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert gateway.calls == [run.steps[0].operation_key]
    assert store.get_workflow_run(run.run_id).steps[0].attempts == 1


def test_heartbeat_prevents_takeover_during_long_request(settings: object) -> None:
    short_lease = replace(  # type: ignore[arg-type]
        settings, lease_ttl_seconds=0.1, lease_renew_interval_seconds=0.02
    )
    store = SQLiteStorage(short_lease.engine_db)
    _create(store)
    gateway = BlockingGateway()
    first = WorkflowEngine(store, gateway, short_lease, worker_id="worker-1")
    second = WorkflowEngine(store, gateway, short_lease, worker_id="worker-2")

    thread = threading.Thread(target=first.run_once)
    thread.start()
    assert gateway.entered.wait(2)
    time.sleep(0.2)
    assert second.run_once().did_work is False
    gateway.release.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(gateway.calls) == 1


def test_two_workers_can_process_different_runs(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    first_run = _create(store, "first")
    second_run = _create(store, "second")
    barrier = threading.Barrier(2)
    calls: list[str] = []
    lock = threading.Lock()

    class ConcurrentGateway:
        def execute(self, step: StepRecord) -> ToolResult:
            with lock:
                calls.append(step.run_id)
            barrier.wait(timeout=2)
            return ToolResult(operation=step.operation, reference_id=step.run_id)

    gateway = ConcurrentGateway()
    engines = [
        WorkflowEngine(store, gateway, settings, worker_id=f"worker-{index}")  # type: ignore[arg-type]
        for index in (1, 2)
    ]
    threads = [threading.Thread(target=engine.run_once) for engine in engines]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert set(calls) == {first_run.run_id, second_run.run_id}
