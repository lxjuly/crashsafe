#!/usr/bin/env python3
"""One deterministic review demo: two workflows, two workers, one ambiguous charge."""

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
from crashsafe.storage import SQLiteStorage

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".crashsafe" / "demo"
API_URL = "http://127.0.0.1:8010"
TOOL_URL = "http://127.0.0.1:8011"
DEMO_PAUSE = float(os.getenv("CRASHSAFE_DEMO_PAUSE", "0"))


def pace() -> None:
    if DEMO_PAUSE:
        time.sleep(DEMO_PAUSE)


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


def load_example(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "examples" / name).read_text(encoding="utf-8"))


def workflow(workflow_id: str) -> dict[str, Any]:
    response = httpx.get(f"{API_URL}/workflows/{workflow_id}")
    response.raise_for_status()
    return response.json()


def main() -> None:
    if STATE.exists():
        shutil.rmtree(STATE)
    STATE.mkdir(parents=True)
    charge_signal = STATE / "charge-committed.signal"
    environment = os.environ.copy()
    environment.update(
        {
            "CRASHSAFE_STATE_DIR": str(STATE),
            "CRASHSAFE_API_PORT": "8010",
            "CRASHSAFE_TOOL_PORT": "8011",
            "CRASHSAFE_TOOL_URL": TOOL_URL,
            "CRASHSAFE_FLAKY_RATE": "0",
            "CRASHSAFE_COMMIT_SIGNAL_FILE": str(charge_signal),
            "CRASHSAFE_COMMIT_DELAY": "30",
            "CRASHSAFE_REQUEST_TIMEOUT": "60",
            "CRASHSAFE_LEASE_TTL": "0.5",
            "CRASHSAFE_LEASE_RENEW_INTERVAL": "0.1",
            "CRASHSAFE_POLL_INTERVAL": "0.02",
        }
    )
    children: list[subprocess.Popen[bytes]] = []
    try:
        print("1. Start the API and an independently durable mock tool", flush=True)
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

        paid_response = httpx.post(
            f"{API_URL}/workflows", json=load_example("paid-onboarding.json")
        )
        trial_response = httpx.post(
            f"{API_URL}/workflows", json=load_example("trial-activation.json")
        )
        paid_response.raise_for_status()
        trial_response.raise_for_status()
        paid = paid_response.json()
        trial = trial_response.json()
        paid_id, trial_id = str(paid["id"]), str(trial["id"])
        charge_key = str(paid["steps"][0]["operation_key"])
        print(f"   paid workflow:  {paid_id}")
        print(f"   trial workflow: {trial_id}")
        print(f"   stable charge key: {charge_key}\n")
        pace()

        print("2. Start two workers; worker-1 owns the paid workflow", flush=True)
        first_env = environment.copy()
        first_env["CRASHSAFE_WORKER_ID"] = "worker-1"
        first_env["CRASHSAFE_DELAY_AFTER_TOOL_COMMIT"] = "charge"
        worker_1 = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.worker", "--until-terminal", paid_id],
            env=first_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.append(worker_1)
        wait_for(charge_signal.exists, "durable charge commit")

        second_env = environment.copy()
        second_env["CRASHSAFE_WORKER_ID"] = "worker-2"
        worker_2 = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.worker"],
            env=second_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.append(worker_2)
        wait_for(
            lambda: workflow(trial_id)["steps"][0]["status"] != "pending",
            "worker-2 trial progress",
        )
        print("   worker-2 concurrently progresses the independent trial workflow\n")
        pace()

        print("3. The tool committed charge; kill worker-1 before engine completion")
        print(f"   ledger before kill: {httpx.get(f'{TOOL_URL}/ledger').json()}")
        print(f"   $ kill -9 {worker_1.pid}", flush=True)
        os.kill(worker_1.pid, signal.SIGKILL)
        if worker_1.wait(timeout=5) != -signal.SIGKILL:
            raise RuntimeError("worker-1 did not exit from SIGKILL")
        interrupted = workflow(paid_id)
        print(f"   engine charge state: {interrupted['steps'][0]['status']}\n")
        pace()

        print("4. Worker-2 takes the expired lease and safely resumes", flush=True)
        wait_for(
            lambda: (
                workflow(paid_id)["status"] == "completed"
                and workflow(trial_id)["status"] == "completed"
            ),
            "both workflows to complete",
        )
        worker_2.send_signal(signal.SIGTERM)
        if worker_2.wait(timeout=5) != 0:
            raise RuntimeError("worker-2 did not drain cleanly")

        events: list[dict[str, Any]] = httpx.get(f"{API_URL}/workflows/{paid_id}/events").json()
        attempts = [
            event
            for event in events
            if event["event_type"] == "StepAttemptStarted" and event["step_id"] == "charge"
        ]
        keys = [event["payload"]["operation_key"] for event in attempts]
        fences = [event["payload"]["fence_token"] for event in attempts]
        ledger = httpx.get(f"{TOOL_URL}/ledger").json()
        store = SQLiteStorage(STATE / "engine.db")
        consistent = all(store.projection_matches_history(item) for item in (paid_id, trial_id))
        print(f"   charge attempts: {[event['attempt'] for event in attempts]}")
        print(f"   fence tokens: {fences}")
        print(f"   same key reused: {keys == [charge_key, charge_key]}")
        print(f"   history reconstructs projections: {consistent}")
        print(f"   final durable ledger: {ledger}\n")
        pace()

        for workflow_id in (paid_id, trial_id):
            timeline = WorkflowTimeline.model_validate(
                httpx.get(f"{API_URL}/workflows/{workflow_id}/timeline").json()
            )
            print(format_timeline(timeline))
            print()
            pace()

        expected_ledger = {"charges": 1, "provisions": 2, "notifications": 3}
        if (
            len(attempts) != 2
            or keys != [charge_key, charge_key]
            or fences != [1, 2]
            or ledger != expected_ledger
            or not consistent
        ):
            raise RuntimeError("demo invariant failed")
        print("PASS — two workflows complete; the ambiguous charge fired exactly once.")
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
        for child in children:
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(timeout=5)


if __name__ == "__main__":
    main()
