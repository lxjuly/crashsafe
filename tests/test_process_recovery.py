from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from crashsafe.models import EventType, StepStatus, WorkflowCreate, WorkflowStatus
from crashsafe.storage import SQLiteStorage


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for(predicate: object, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():  # type: ignore[operator]
            return
        time.sleep(0.02)
    raise AssertionError("condition did not become true before timeout")


@pytest.fixture
def live_tool(tmp_path: Path) -> Iterator[dict[str, str]]:
    port = free_port()
    signal_file = tmp_path / "committed.signal"
    env = os.environ.copy()
    env.update(
        {
            "CRASHSAFE_STATE_DIR": str(tmp_path),
            "CRASHSAFE_TOOL_PORT": str(port),
            "CRASHSAFE_TOOL_URL": f"http://127.0.0.1:{port}",
            "CRASHSAFE_FLAKY_RATE": "0",
            "CRASHSAFE_COMMIT_SIGNAL_FILE": str(signal_file),
            "CRASHSAFE_COMMIT_DELAY": "1",
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "crashsafe.mock_tool"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    def healthy() -> bool:
        try:
            return httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=0.2).is_success
        except httpx.RequestError:
            return False

    wait_for(healthy)
    try:
        yield env
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)


def test_sigkill_in_ambiguous_charge_window_recovers_exactly_one_effect(
    tmp_path: Path, live_tool: dict[str, str]
) -> None:
    env = live_tool
    store = SQLiteStorage(tmp_path / "engine.db")
    workflow = store.create_workflow(
        WorkflowCreate(customer_id="customer-1", amount_cents=2500, email="a@example.com")
    )
    first_env = env.copy()
    first_env["CRASHSAFE_DELAY_AFTER_TOOL_COMMIT"] = "charge"
    first_env["CRASHSAFE_REQUEST_TIMEOUT"] = "60"
    worker = subprocess.Popen(
        [sys.executable, "-m", "crashsafe.worker", "--until-terminal", workflow.id],
        env=first_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    signal_file = Path(env["CRASHSAFE_COMMIT_SIGNAL_FILE"])
    wait_for(signal_file.exists)
    os.kill(worker.pid, signal.SIGKILL)
    assert worker.wait(timeout=5) == -signal.SIGKILL

    interrupted = store.get_workflow(workflow.id)
    assert interrupted.steps[0].status == StepStatus.INTENT_RECORDED
    assert [event.event_type for event in store.list_events(workflow.id)] == [
        EventType.WORKFLOW_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
    ]
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json()["charges"] == 1

    restart_env = env.copy()
    restart_env.pop("CRASHSAFE_DELAY_AFTER_TOOL_COMMIT", None)
    recovered = subprocess.run(
        [
            sys.executable,
            "-m",
            "crashsafe.worker",
            "--until-terminal",
            workflow.id,
            "--timeout",
            "10",
        ],
        env=restart_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
        check=False,
    )
    assert recovered.returncode == 0
    finished = store.get_workflow(workflow.id)
    assert finished.status == WorkflowStatus.COMPLETED
    assert finished.steps[0].attempts == 2
    charge_attempts = [
        event
        for event in store.list_events(workflow.id)
        if event.event_type == EventType.STEP_ATTEMPT_STARTED
        and event.step_id == finished.steps[0].id
    ]
    assert [event.attempt for event in charge_attempts] == [1, 2]
    assert [event.schema_version for event in charge_attempts] == [2, 2]
    assert charge_attempts[0].payload["fence_token"] == 1
    assert charge_attempts[1].payload["fence_token"] == 2
    assert charge_attempts[0].payload["worker_id"] != charge_attempts[1].payload["worker_id"]
    assert {
        str(event.payload["operation_key"])
        for event in charge_attempts
    } == {finished.steps[0].operation_key}
    assert store.audit_workflow(workflow.id).consistent
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json() == {
        "charges": 1,
        "provisions": 1,
        "notifications": 1,
    }


def test_sigkill_between_event_append_and_projection_update_rolls_back_transaction(
    tmp_path: Path, live_tool: dict[str, str]
) -> None:
    env = live_tool
    store = SQLiteStorage(tmp_path / "engine.db")
    workflow = store.create_workflow(
        WorkflowCreate(customer_id="atomic-test", amount_cents=2500, email="a@example.com")
    )
    transition_signal = tmp_path / "step-completed-uncommitted.signal"
    crashing_env = env.copy()
    crashing_env.update(
        {
            "CRASHSAFE_PAUSE_AFTER_EVENT": EventType.STEP_COMPLETED.value,
            "CRASHSAFE_EVENT_SIGNAL_FILE": str(transition_signal),
            "CRASHSAFE_EVENT_PAUSE_SECONDS": "30",
        }
    )
    worker = subprocess.Popen(
        [sys.executable, "-m", "crashsafe.worker", "--once"],
        env=crashing_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_for(transition_signal.exists)
    os.kill(worker.pid, signal.SIGKILL)
    assert worker.wait(timeout=5) == -signal.SIGKILL

    interrupted = store.get_workflow(workflow.id)
    assert interrupted.steps[0].status == StepStatus.INTENT_RECORDED
    assert [event.event_type for event in store.list_events(workflow.id)] == [
        EventType.WORKFLOW_CREATED,
        EventType.STEP_ATTEMPT_STARTED,
    ]
    assert store.audit_workflow(workflow.id).consistent
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json()["charges"] == 1

    recovered = subprocess.run(
        [sys.executable, "-m", "crashsafe.worker", "--until-terminal", workflow.id],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=10,
        check=False,
    )
    assert recovered.returncode == 0
    assert store.get_workflow(workflow.id).status == WorkflowStatus.COMPLETED
    assert store.audit_workflow(workflow.id).consistent
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json()["charges"] == 1


def test_sigterm_drains_in_flight_step_then_restart_resumes(
    tmp_path: Path, live_tool: dict[str, str]
) -> None:
    env = live_tool
    store = SQLiteStorage(tmp_path / "engine.db")
    workflow = store.create_workflow(
        WorkflowCreate(customer_id="drain-test", amount_cents=2500, email="a@example.com")
    )
    draining_env = env.copy()
    draining_env["CRASHSAFE_DELAY_AFTER_TOOL_COMMIT"] = "charge"
    draining_env["CRASHSAFE_REQUEST_TIMEOUT"] = "5"
    worker = subprocess.Popen(
        [sys.executable, "-m", "crashsafe.worker"],
        env=draining_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    signal_file = Path(env["CRASHSAFE_COMMIT_SIGNAL_FILE"])
    wait_for(signal_file.exists)
    os.kill(worker.pid, signal.SIGTERM)
    assert worker.wait(timeout=5) == 0

    drained = store.get_workflow(workflow.id)
    assert drained.status == WorkflowStatus.RUNNING
    assert drained.steps[0].status == StepStatus.COMPLETED
    assert drained.steps[1].status == StepStatus.PENDING
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json()["charges"] == 1

    restart_env = env.copy()
    restart_env.pop("CRASHSAFE_DELAY_AFTER_TOOL_COMMIT", None)
    recovered = subprocess.run(
        [sys.executable, "-m", "crashsafe.worker", "--until-terminal", workflow.id],
        env=restart_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=10,
        check=False,
    )
    assert recovered.returncode == 0
    assert store.get_workflow(workflow.id).status == WorkflowStatus.COMPLETED
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json() == {
        "charges": 1,
        "provisions": 1,
        "notifications": 1,
    }


@pytest.mark.parametrize(
    ("crash_point", "expected_status", "expected_charges"),
    [
        ("charge:before_request", StepStatus.INTENT_RECORDED, 0),
        ("charge:after_response", StepStatus.INTENT_RECORDED, 1),
        ("charge:after_commit", StepStatus.COMPLETED, 1),
    ],
)
def test_deterministic_hard_crash_points_resume(
    tmp_path: Path,
    live_tool: dict[str, str],
    crash_point: str,
    expected_status: StepStatus,
    expected_charges: int,
) -> None:
    env = live_tool
    store = SQLiteStorage(tmp_path / "engine.db")
    workflow = store.create_workflow(
        WorkflowCreate(customer_id=crash_point, amount_cents=100, email="a@example.com")
    )
    crashing_env = env.copy()
    crashing_env["CRASHSAFE_CRASH_AT"] = crash_point
    crashed = subprocess.run(
        [sys.executable, "-m", "crashsafe.worker", "--once"],
        env=crashing_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=5,
        check=False,
    )
    assert crashed.returncode == 91
    assert store.get_workflow(workflow.id).steps[0].status == expected_status
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json()["charges"] == expected_charges

    recovered = subprocess.run(
        [sys.executable, "-m", "crashsafe.worker", "--until-terminal", workflow.id],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=10,
        check=False,
    )
    assert recovered.returncode == 0
    assert store.get_workflow(workflow.id).status == WorkflowStatus.COMPLETED
    assert httpx.get(f"{env['CRASHSAFE_TOOL_URL']}/ledger").json()["charges"] == 1
