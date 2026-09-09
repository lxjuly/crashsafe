#!/usr/bin/env python3
"""Deterministic graceful-drain scenario with before/after timelines."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

import httpx

from crashsafe.models import WorkflowRunTimeline
from crashsafe.observability import format_timeline
from crashsafe.storage import SQLiteStorage

ROOT = Path(__file__).resolve().parents[1]
STATE = Path(os.getenv("CRASHSAFE_STATE_DIR", ROOT / ".crashsafe")).resolve()


def wait_for(predicate: Callable[[], bool], label: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.03)
    raise RuntimeError(f"timed out waiting for {label}")


def healthy(url: str) -> bool:
    try:
        return httpx.get(url, timeout=0.2).is_success
    except httpx.RequestError:
        return False


def available_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def wait_for_service(process: subprocess.Popen[bytes], url: str, label: str) -> None:
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        if healthy(f"{url}/healthz"):
            return
        return_code = process.poll()
        if return_code is not None:
            output = ""
            if process.stdout is not None:
                output = process.stdout.read().decode(errors="replace").strip()
            detail = f"\n{output}" if output else ""
            raise RuntimeError(f"{label} exited during startup ({return_code}){detail}")
        time.sleep(0.03)
    raise RuntimeError(f"timed out waiting for {label} at {url}")


def workflow_run(api_url: str, run_id: str) -> dict[str, Any]:
    response = httpx.get(f"{api_url}/workflow_runs/{run_id}")
    response.raise_for_status()
    return response.json()


def workflow_events(api_url: str, run_id: str) -> list[dict[str, Any]]:
    response = httpx.get(f"{api_url}/workflow_runs/{run_id}/events")
    response.raise_for_status()
    return response.json()


def workflow_timeline(api_url: str, run_id: str) -> WorkflowRunTimeline:
    response = httpx.get(f"{api_url}/workflow_runs/{run_id}/timeline")
    response.raise_for_status()
    return WorkflowRunTimeline.model_validate(response.json())


def main() -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    commit_signal = STATE / "charge-committed.signal"
    commit_signal.unlink(missing_ok=True)
    unfinished = [
        run.run_id
        for run in SQLiteStorage(STATE / "engine.db").list_workflow_runs(limit=10_000)
        if run.status.value == "running"
    ]
    if unfinished:
        raise RuntimeError(
            f"unfinished graceful-drain runs require recovery: {unfinished}; "
            f"finish them or manually clear {STATE}"
        )
    api_port = available_port()
    tool_port = available_port()
    while tool_port == api_port:
        tool_port = available_port()
    api_url = f"http://127.0.0.1:{api_port}"
    tool_url = f"http://127.0.0.1:{tool_port}"
    environment = os.environ.copy()
    environment.update(
        {
            "CRASHSAFE_STATE_DIR": str(STATE),
            "CRASHSAFE_API_PORT": str(api_port),
            "CRASHSAFE_TOOL_PORT": str(tool_port),
            "CRASHSAFE_TOOL_URL": tool_url,
            "CRASHSAFE_FLAKY_RATE": "0",
            "CRASHSAFE_COMMIT_SIGNAL_FILE": str(commit_signal),
            "CRASHSAFE_COMMIT_DELAY": "2",
            "CRASHSAFE_REQUEST_TIMEOUT": "5",
            "CRASHSAFE_POLL_INTERVAL": "0.02",
        }
    )
    children: list[subprocess.Popen[bytes]] = []
    try:
        print("1. Start isolated API and tool services", flush=True)
        tool = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.mock_tool"],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        api = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.api"],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        children.extend([tool, api])
        wait_for_service(tool, tool_url, "tool")
        wait_for_service(api, api_url, "API")
        print(f"   API:  {api_url}")
        print(f"   tool: {tool_url}")
        baseline_ledger = httpx.get(f"{tool_url}/ledger").json()

        definition = json.loads(
            (ROOT / "workflows" / "paid-onboarding.json").read_text(encoding="utf-8")
        )
        response = httpx.post(f"{api_url}/workflow_runs", json=definition)
        response.raise_for_status()
        run_id = str(response.json()["run_id"])
        print(f"   workflow run: {run_id}\n")

        print("2. Start a worker and wait for its charge to commit", flush=True)
        worker_environment = environment.copy()
        worker_environment["CRASHSAFE_WORKER_ID"] = "worker-draining"
        worker_environment["CRASHSAFE_DELAY_AFTER_TOOL_COMMIT"] = "charge"
        worker = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.worker"],
            env=worker_environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.append(worker)
        wait_for(commit_signal.exists, "durable charge commit")

        print(f"3. Send SIGTERM to worker {worker.pid} during the in-flight response")
        worker.send_signal(signal.SIGTERM)
        time.sleep(0.2)
        if worker.poll() is not None:
            raise RuntimeError("worker exited before its in-flight attempt completed")
        print("   worker remains alive to finish and persist the current attempt", flush=True)
        if worker.wait(timeout=5) != 0:
            raise RuntimeError("draining worker did not exit cleanly")

        drained = workflow_run(api_url, run_id)
        if drained["status"] != "running" or drained["steps"][0]["status"] != "completed":
            raise RuntimeError("drain did not stop after committing only the in-flight step")
        print("\n4. Timeline after the original worker drains")
        print(format_timeline(workflow_timeline(api_url, run_id)))

        print("\n5. Start a replacement worker to finish the run", flush=True)
        resumed_environment = environment.copy()
        resumed_environment["CRASHSAFE_WORKER_ID"] = "worker-resumed"
        resumed = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.worker", "--until-terminal", run_id],
            env=resumed_environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.append(resumed)
        if resumed.wait(timeout=15) != 0:
            raise RuntimeError("replacement worker did not complete the run")

        timeline = workflow_timeline(api_url, run_id)
        events = workflow_events(api_url, run_id)
        ledger = httpx.get(f"{tool_url}/ledger").json()
        ledger_delta = {
            operation: int(ledger[operation]) - int(baseline_ledger[operation])
            for operation in ("charges", "provisions", "notifications")
        }
        charge_attempts = [
            event
            for event in events
            if event["event_type"] == "StepAttemptStarted" and event["step_id"] == "charge"
        ]
        consistent = SQLiteStorage(STATE / "engine.db").projection_matches_history(run_id)

        print("\n6. Final timeline after restart")
        print(format_timeline(timeline))
        print(f"\n   durable ledger: {ledger}")
        print(f"   side effects added by this check: {ledger_delta}")

        expected_delta = {"charges": 1, "provisions": 1, "notifications": 1}
        if len(charge_attempts) != 1 or ledger_delta != expected_delta or not consistent:
            raise RuntimeError("graceful-drain invariant failed")
        print("PASS — SIGTERM committed only the in-flight attempt; restart completed the run.")
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
        for child in children:
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(timeout=5)


if __name__ == "__main__":
    main()
