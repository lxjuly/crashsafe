#!/usr/bin/env python3
"""Core demo: concurrent 429 handling plus ambiguous-charge crash recovery."""

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

from crashsafe.models import WorkflowRunTimeline
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


def load_workflow_definition(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "workflows" / name).read_text(encoding="utf-8"))


def workflow_run(run_id: str) -> dict[str, Any]:
    response = httpx.get(f"{API_URL}/workflow_runs/{run_id}")
    response.raise_for_status()
    return response.json()


def workflow_events(run_id: str) -> list[dict[str, Any]]:
    response = httpx.get(f"{API_URL}/workflow_runs/{run_id}/events")
    response.raise_for_status()
    return response.json()


def workflow_timeline(run_id: str) -> WorkflowRunTimeline:
    response = httpx.get(f"{API_URL}/workflow_runs/{run_id}/timeline")
    response.raise_for_status()
    return WorkflowRunTimeline.model_validate(response.json())


def print_timelines(label: str, run_ids: tuple[str, str]) -> None:
    print(label)
    for run_id in run_ids:
        print(format_timeline(workflow_timeline(run_id)))
        print()


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
            "CRASHSAFE_FAIL_FIRST_OPERATION": "provision",
            "CRASHSAFE_RETRY_AFTER": "2",
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
        print("1. Start the API and independently durable mock tool", flush=True)
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
            f"{API_URL}/workflow_runs", json=load_workflow_definition("paid-onboarding.json")
        )
        trial_response = httpx.post(
            f"{API_URL}/workflow_runs", json=load_workflow_definition("trial-activation.json")
        )
        paid_response.raise_for_status()
        trial_response.raise_for_status()
        paid = paid_response.json()
        trial = trial_response.json()
        paid_id, trial_id = str(paid["run_id"]), str(trial["run_id"])
        charge_key = str(paid["steps"][0]["operation_key"])
        print(f"   crash-recovery run: {paid_id}")
        print(f"   Retry-After run:    {trial_id}")
        print(f"   stable charge key: {charge_key}\n")
        pace()

        print("2. Worker-1 starts the charge and blocks after its durable commit", flush=True)
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

        print("   worker-1 is still inside the charge call")
        print("   start worker-2 on the independent trial run", flush=True)
        second_env = environment.copy()
        second_env["CRASHSAFE_WORKER_ID"] = "worker-2"
        worker_2 = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.worker", "--once"],
            env=second_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.append(worker_2)
        wait_for(
            lambda: any(
                event["event_type"] == "StepRetryScheduled" for event in workflow_events(trial_id)
            ),
            "persisted HTTP 429 retry",
        )
        if worker_2.wait(timeout=5) != 0:
            raise RuntimeError("worker-2 did not finish its single 429 attempt")
        retry_event = next(
            event
            for event in workflow_events(trial_id)
            if event["event_type"] == "StepRetryScheduled"
        )
        print("   worker-2 received HTTP 429 while worker-1 remained in flight")
        print(f"   persisted next_attempt_at: {retry_event['payload']['next_attempt_at']}\n")
        pace()

        print("3. Kill worker-1 after the 429 but before charge completion is recorded")
        print(f"   ledger before kill: {httpx.get(f'{TOOL_URL}/ledger').json()}")
        print(f"   $ kill -9 {worker_1.pid}", flush=True)
        os.kill(worker_1.pid, signal.SIGKILL)
        if worker_1.wait(timeout=5) != -signal.SIGKILL:
            raise RuntimeError("worker-1 did not exit from SIGKILL")
        interrupted = workflow_run(paid_id)
        print(f"   engine charge state: {interrupted['steps'][0]['status']}\n")
        pace()

        print_timelines("4. Both timelines immediately after SIGKILL", (paid_id, trial_id))
        pace()

        print("5. Restart one worker; durable state drives both runs to completion", flush=True)
        resumed_env = environment.copy()
        resumed_env["CRASHSAFE_WORKER_ID"] = "worker-resumed"
        worker_resumed = subprocess.Popen(
            [sys.executable, "-m", "crashsafe.worker"],
            env=resumed_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        children.append(worker_resumed)
        wait_for(
            lambda: (
                workflow_run(paid_id)["status"] == "completed"
                and workflow_run(trial_id)["status"] == "completed"
            ),
            "both runs to complete",
        )
        worker_resumed.send_signal(signal.SIGTERM)
        if worker_resumed.wait(timeout=5) != 0:
            raise RuntimeError("resumed worker did not stop cleanly")

        events = workflow_events(paid_id)
        attempts = [
            event
            for event in events
            if event["event_type"] == "StepAttemptStarted" and event["step_id"] == "charge"
        ]
        keys = [event["payload"]["operation_key"] for event in attempts]
        fences = [event["payload"]["fence_token"] for event in attempts]
        retry_timeline = workflow_timeline(trial_id)
        retry_attempts = [
            event
            for event in workflow_events(trial_id)
            if event["event_type"] == "StepAttemptStarted" and event["step_id"] == "provision-trial"
        ]
        ledger = httpx.get(f"{TOOL_URL}/ledger").json()
        store = SQLiteStorage(STATE / "engine.db")
        consistent = all(store.projection_matches_history(item) for item in (paid_id, trial_id))
        print(f"   charge attempts: {[event['attempt'] for event in attempts]}")
        print(f"   fence tokens: {fences}")
        print(f"   same key reused: {keys == [charge_key, charge_key]}")
        print(f"   429 retries: {retry_timeline.summary.retries}")
        print(f"   honored Retry-After: {retry_timeline.summary.planned_wait_ms}ms")
        print(f"   history reconstructs projections: {consistent}")
        print(f"   final durable ledger: {ledger}\n")
        pace()

        print_timelines("6. Both timelines after recovery and completion", (paid_id, trial_id))
        pace()

        expected_ledger = {"charges": 1, "provisions": 2, "notifications": 3}
        if (
            len(attempts) != 2
            or keys != [charge_key, charge_key]
            or fences != [1, 2]
            or len(retry_attempts) != 2
            or retry_timeline.summary.retries != 1
            or retry_timeline.summary.planned_wait_ms < 1900
            or ledger != expected_ledger
            or not consistent
        ):
            raise RuntimeError("demo invariant failed")
        print("PASS — crash recovery fired one charge; the other run honored Retry-After.")
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
        for child in children:
            with contextlib.suppress(subprocess.TimeoutExpired):
                child.wait(timeout=5)


if __name__ == "__main__":
    main()
