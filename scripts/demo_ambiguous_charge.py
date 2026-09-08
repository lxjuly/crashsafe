#!/usr/bin/env python3
"""Reproduce the hardest crash window with a real SIGKILL."""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

import httpx

from crashsafe.models import WorkflowTimeline
from crashsafe.observability import format_timeline

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".crashsafe" / "demo"
API_URL = "http://127.0.0.1:8010"
TOOL_URL = "http://127.0.0.1:8011"
DEMO_PAUSE = float(os.environ.get("CRASHSAFE_DEMO_PAUSE", "0"))


def pace() -> None:
    if DEMO_PAUSE > 0:
        time.sleep(DEMO_PAUSE)


def wait_for(predicate: Callable[[], bool], label: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {label}")


def healthy(url: str) -> bool:
    try:
        return httpx.get(url, timeout=0.2).is_success
    except httpx.RequestError:
        return False


def pretty(value: Any) -> str:
    return json.dumps(value, indent=2)


def event_summary(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "sequence": event["sequence"],
            "type": event["event_type"],
            "step_id": event["step_id"],
            "attempt": event["attempt"],
            "operation_key": event["payload"].get("operation_key"),
        }
        for event in events
    ]


def main() -> None:
    if STATE.exists():
        shutil.rmtree(STATE)
    STATE.mkdir(parents=True)
    signal_file = STATE / "charge-committed.signal"
    environment = os.environ.copy()
    environment.update(
        {
            "CRASHSAFE_STATE_DIR": str(STATE),
            "CRASHSAFE_API_PORT": "8010",
            "CRASHSAFE_TOOL_PORT": "8011",
            "CRASHSAFE_TOOL_URL": TOOL_URL,
            "CRASHSAFE_FLAKY_RATE": "0",
            "CRASHSAFE_COMMIT_SIGNAL_FILE": str(signal_file),
            "CRASHSAFE_COMMIT_DELAY": "30",
            "CRASHSAFE_REQUEST_TIMEOUT": "60",
        }
    )
    children: list[subprocess.Popen[bytes]] = []
    try:
        print("CHECKPOINT 1 — start isolated durability domains", flush=True)
        tool = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.mock_tool"],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        api = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.api"],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.extend([tool, api])
        wait_for(lambda: healthy(f"{API_URL}/healthz"), "API")
        wait_for(lambda: healthy(f"{TOOL_URL}/healthz"), "tool")

        created: dict[str, Any] = httpx.post(
            f"{API_URL}/workflows",
            json={
                "customer_id": "demo-customer",
                "amount_cents": 4200,
                "email": "demo@example.com",
            },
        ).json()
        workflow_id = str(created["id"])
        operation_key = str(created["steps"][0]["operation_key"])
        print(f"workflow: {workflow_id}")
        print(f"stable charge key: {operation_key}\n")
        pace()

        print("CHECKPOINT 2 — commit charge, then kill before engine completion", flush=True)
        crash_environment = environment.copy()
        crash_environment["CRASHSAFE_WORKER_ID"] = "worker-1"
        crash_environment["CRASHSAFE_DELAY_AFTER_TOOL_COMMIT"] = "charge"
        worker = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.worker", "--until-terminal", workflow_id],
            env=crash_environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.append(worker)
        wait_for(signal_file.exists, "durable charge commit")
        before = httpx.get(f"{TOOL_URL}/ledger").json()
        print(f"tool ledger before kill: {before}")
        print(f"$ kill -9 {worker.pid}  # worker has not received the response")
        pace()
        os.kill(worker.pid, signal.SIGKILL)
        if worker.wait(timeout=5) != -signal.SIGKILL:
            raise RuntimeError("worker did not exit from SIGKILL")
        print(f"worker {worker.pid} exited from SIGKILL")

        interrupted = httpx.get(f"{API_URL}/workflows/{workflow_id}").json()
        interrupted_events: list[dict[str, Any]] = httpx.get(
            f"{API_URL}/workflows/{workflow_id}/events"
        ).json()
        print(f"engine charge state: {interrupted['steps'][0]['status']}")
        print("history before recovery:")
        print(pretty(event_summary(interrupted_events)))
        if any(event["event_type"] == "StepCompleted" for event in interrupted_events):
            raise RuntimeError("charge completion unexpectedly committed before the kill")
        print()
        pace()

        print("CHECKPOINT 3 — recover unknown outcome with the same key", flush=True)
        pace()
        recovery_environment = environment.copy()
        recovery_environment["CRASHSAFE_WORKER_ID"] = "worker-2"
        recovered = subprocess.run(
            [
                sys.executable,
                "-m",
                "crashsafe.worker",
                "--until-terminal",
                workflow_id,
                "--timeout",
                "10",
            ],
            env=recovery_environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            timeout=15,
            check=False,
        )
        if recovered.returncode != 0:
            raise RuntimeError(f"recovery worker exited {recovered.returncode}")

        finished = httpx.get(f"{API_URL}/workflows/{workflow_id}").json()
        final_events: list[dict[str, Any]] = httpx.get(
            f"{API_URL}/workflows/{workflow_id}/events"
        ).json()
        audit = httpx.get(f"{API_URL}/workflows/{workflow_id}/audit").json()
        timeline = WorkflowTimeline.model_validate(
            httpx.get(f"{API_URL}/workflows/{workflow_id}/timeline").json()
        )
        ledger = httpx.get(f"{TOOL_URL}/ledger").json()
        charge_attempts = [
            event
            for event in final_events
            if event["event_type"] == "StepAttemptStarted"
            and event["step_id"] == created["steps"][0]["id"]
        ]
        attempt_keys = [str(event["payload"]["operation_key"]) for event in charge_attempts]
        print(f"charge attempts: {[event['attempt'] for event in charge_attempts]}")
        print(f"operation keys: {attempt_keys}")
        print()
        print("CHECKPOINT 4 — verify terminal state and invariants")
        print(f"workflow status: {finished['status']}")
        print(f"event/projection audit consistent: {audit['consistent']}")
        print(f"final durable ledger: {ledger}")
        print()
        print(format_timeline(timeline))
        if (
            finished["status"] != "completed"
            or ledger != {"charges": 1, "provisions": 1, "notifications": 1}
            or not audit["consistent"]
            or len(charge_attempts) != 2
            or attempt_keys != [operation_key, operation_key]
        ):
            raise RuntimeError("demo invariant failed")
        print()
        print("PASS — at-least-once request, reconstructed state, exactly one charge effect.")
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
        for child in children:
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(timeout=5)


if __name__ == "__main__":
    main()
