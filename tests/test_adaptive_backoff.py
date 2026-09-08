from __future__ import annotations

from datetime import timedelta

from crashsafe.engine import RetryableToolError, WorkflowEngine
from crashsafe.models import StepRecord, ToolResult, WorkflowCreate
from crashsafe.storage import SQLiteStorage, utc_now


class ThrottleOnceGateway:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, step: StepRecord) -> ToolResult:
        self.calls += 1
        if self.calls == 1:
            raise RetryableToolError(
                "shared throttle", retry_after_seconds=0.15, throttle_tool=True
            )
        return ToolResult(operation=step.name, reference_id="ok")


def test_retry_after_blocks_same_tool_across_workflows_and_restart(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    for customer in ("first", "second"):
        store.create_workflow(
            WorkflowCreate(
                customer_id=customer,
                amount_cents=100,
                email=f"{customer}@example.com",
            )
        )
    gateway = ThrottleOnceGateway()
    now = [utc_now()]
    engine = WorkflowEngine(  # type: ignore[arg-type]
        store, gateway, settings, worker_id="worker-1", clock=lambda: now[0]
    )

    assert engine.run_once().did_work
    throttle = store.get_tool_throttle("mock-tool")
    assert throttle is not None
    assert engine.run_once().did_work is False

    reopened = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    restarted = WorkflowEngine(
        reopened,  # type: ignore[arg-type]
        gateway,
        settings,
        worker_id="worker-after-restart",
        clock=lambda: now[0],
    )
    assert restarted.run_once().did_work is False
    now[0] += timedelta(seconds=0.17)
    assert restarted.run_once().did_work
    assert gateway.calls == 2


def test_non_429_retry_does_not_create_shared_throttle(settings: object) -> None:
    class TransportFailure:
        def execute(self, step: StepRecord) -> ToolResult:
            del step
            raise RetryableToolError("connection reset", retry_after_seconds=0.01)

    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    store.create_workflow(
        WorkflowCreate(customer_id="one", amount_cents=100, email="one@example.com")
    )
    engine = WorkflowEngine(store, TransportFailure(), settings, worker_id="worker-1")  # type: ignore[arg-type]
    engine.run_once()
    assert store.get_tool_throttle("mock-tool") is None


def test_longer_deadline_extends_throttle_and_shorter_cannot_reduce_it(
    settings: object,
) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    first = store.create_workflow(
        WorkflowCreate(customer_id="one", amount_cents=100, email="one@example.com")
    )
    second = store.create_workflow(
        WorkflowCreate(customer_id="two", amount_cents=100, email="two@example.com")
    )
    long_deadline = utc_now() + timedelta(seconds=30)
    short_deadline = utc_now() + timedelta(seconds=10)

    store.record_attempt(first.steps[0].id)
    store.schedule_retry(first.steps[0].id, "long", long_deadline, throttle_tool=True)
    store.record_attempt(second.steps[0].id)
    store.schedule_retry(second.steps[0].id, "short", short_deadline, throttle_tool=True)

    throttle = store.get_tool_throttle("mock-tool")
    assert throttle is not None
    assert throttle.blocked_until == long_deadline
    assert throttle.reason == "long"


def test_throttle_does_not_block_a_different_tool(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    blocked = store.create_workflow(
        WorkflowCreate(customer_id="one", amount_cents=100, email="one@example.com")
    )
    other = store.create_workflow(
        WorkflowCreate(customer_id="two", amount_cents=100, email="two@example.com"),
        tool_key="other-tool",
    )
    store.record_attempt(blocked.steps[0].id)
    store.schedule_retry(
        blocked.steps[0].id,
        "blocked",
        utc_now() + timedelta(seconds=30),
        throttle_tool=True,
    )

    claim = store.claim_runnable_step("worker", 1.0)
    assert claim is not None
    assert claim.step.workflow_id == other.id
