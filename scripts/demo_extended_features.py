#!/usr/bin/env python3
"""Run two workers, force a shared 429, and print history-derived timelines."""

from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

import httpx

from crashsafe.models import WorkflowTimeline
from crashsafe.observability import format_timeline

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".crashsafe" / "features-demo"
API_URL = "http://127.0.0.1:8020"
TOOL_URL = "http://127.0.0.1:8021"


def wait_for(predicate: Callable[[], bool], label: str, timeout: float = 15.0) -> None:
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


def main() -> None:
    if STATE.exists():
        shutil.rmtree(STATE)
    STATE.mkdir(parents=True)
    environment = os.environ.copy()
    environment.update(
        {
            "CRASHSAFE_STATE_DIR": str(STATE),
            "CRASHSAFE_API_PORT": "8020",
            "CRASHSAFE_TOOL_PORT": "8021",
            "CRASHSAFE_TOOL_URL": TOOL_URL,
            "CRASHSAFE_FLAKY_RATE": "0",
            "CRASHSAFE_FAIL_FIRST_N": "1",
            "CRASHSAFE_RETRY_AFTER": "0.5",
            "CRASHSAFE_POLL_INTERVAL": "0.02",
        }
    )
    children: list[subprocess.Popen[bytes]] = []
    try:
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

        workflow_ids: list[str] = []
        for customer in ("concurrent-a", "concurrent-b"):
            response = httpx.post(
                f"{API_URL}/workflows",
                json={
                    "customer_id": customer,
                    "amount_cents": 4200,
                    "email": f"{customer}@example.com",
                },
            )
            response.raise_for_status()
            workflow_ids.append(str(response.json()["id"]))

        print("Starting two independently polling workers.", flush=True)
        for index in (1, 2):
            worker_env = environment.copy()
            worker_env["CRASHSAFE_WORKER_ID"] = f"worker-{index}"
            children.append(
                subprocess.Popen([sys.executable, "-m", "crashsafe.worker"], env=worker_env)
            )

        def all_terminal() -> bool:
            return all(
                httpx.get(f"{API_URL}/workflows/{workflow_id}").json()["status"]
                == "completed"
                for workflow_id in workflow_ids
            )

        wait_for(all_terminal, "both workflows")
        for child in children[2:]:
            child.send_signal(signal.SIGTERM)
        for child in children[2:]:
            if child.wait(timeout=5) != 0:
                raise RuntimeError("worker did not drain cleanly")

        owners: set[str] = set()
        retry_count = 0
        for workflow_id in workflow_ids:
            timeline = WorkflowTimeline.model_validate(
                httpx.get(f"{API_URL}/workflows/{workflow_id}/timeline").json()
            )
            print()
            print(format_timeline(timeline))
            owners.update(
                entry.worker_id for entry in timeline.entries if entry.worker_id is not None
            )
            retry_count += timeline.summary.retries
            if not timeline.summary.audit_consistent:
                raise RuntimeError("history and projection diverged")

        ledger = httpx.get(f"{TOOL_URL}/ledger").json()
        print()
        print(f"workers observed: {sorted(owners)}")
        print(f"forced Retry-After events: {retry_count}")
        print(f"durable ledger: {ledger}")
        if len(owners) != 2 or retry_count != 1 or ledger != {
            "charges": 2,
            "provisions": 2,
            "notifications": 2,
        }:
            raise RuntimeError("extended feature demo invariant failed")
        print("PASS — fenced concurrency, shared Retry-After, drain, and timeline verified.")
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
        for child in children:
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(timeout=5)


if __name__ == "__main__":
    main()
