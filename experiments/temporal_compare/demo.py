from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import httpx
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowHandle

from experiments.temporal_compare.workflow import TASK_QUEUE, OrderInput, OrderWorkflow

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / ".crashsafe" / "temporal-demo"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def wait_for_http(url: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient(timeout=0.2) as client:
        while time.monotonic() < deadline:
            try:
                if (await client.get(url)).is_success:
                    return
            except httpx.RequestError:
                pass
            await asyncio.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {url}")


async def wait_for_file(path: Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        await asyncio.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {path}")


async def connect_temporal(address: str, timeout: float = 10.0) -> Client:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return await Client.connect(address)
        except RuntimeError:
            await asyncio.sleep(0.1)
    raise RuntimeError(f"timed out connecting to Temporal at {address}")


async def history_summary(handle: WorkflowHandle[Any, Any]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    async for event in handle.fetch_history_events():
        name = EventType.Name(event.event_type)
        if name not in {
            "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
            "EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
            "EVENT_TYPE_ACTIVITY_TASK_STARTED",
            "EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT",
            "EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
            "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED",
        }:
            continue
        attempt: Optional[int] = None
        previous_failure: Optional[str] = None
        if event.HasField("activity_task_started_event_attributes"):
            attributes = event.activity_task_started_event_attributes
            attempt = attributes.attempt
            if attributes.HasField("last_failure"):
                previous_failure = attributes.last_failure.message
        summary.append(
            {
                "event_id": event.event_id,
                "type": name.removeprefix("EVENT_TYPE_"),
                "attempt": attempt,
                "previous_failure": previous_failure,
            }
        )
    return summary


async def ledger(tool_url: str) -> dict[str, int]:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{tool_url}/ledger")
        response.raise_for_status()
        result: dict[str, int] = response.json()
        return result


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=5)
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)


async def run() -> None:
    sys.stdout.reconfigure(line_buffering=True)
    if STATE.exists():
        shutil.rmtree(STATE)
    STATE.mkdir(parents=True)
    temporal_port = free_port()
    tool_port = free_port()
    temporal_address = f"127.0.0.1:{temporal_port}"
    tool_url = f"http://127.0.0.1:{tool_port}"
    commit_signal = STATE / "charge-committed.signal"
    environment = os.environ.copy()
    environment.update(
        {
            "TEMPORAL_ADDRESS": temporal_address,
            "CRASHSAFE_STATE_DIR": str(STATE),
            "CRASHSAFE_TOOL_PORT": str(tool_port),
            "CRASHSAFE_TOOL_URL": tool_url,
            "CRASHSAFE_FLAKY_RATE": "0",
            "CRASHSAFE_COMMIT_SIGNAL_FILE": str(commit_signal),
            "CRASHSAFE_COMMIT_DELAY": "30",
            "PYTHONPATH": str(ROOT),
        }
    )
    temporal_server = subprocess.Popen(
        [
            "temporal",
            "server",
            "start-dev",
            "--headless",
            "--ip",
            "127.0.0.1",
            "--port",
            str(temporal_port),
            "--db-filename",
            str(STATE / "temporal.db"),
            "--log-level",
            "error",
        ],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    tool = subprocess.Popen(
        [sys.executable, "-m", "crashsafe.mock_tool"],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    workers: list[subprocess.Popen[bytes]] = []
    try:
        client = await connect_temporal(temporal_address)
        await wait_for_http(f"{tool_url}/healthz")
        workflow_id = f"temporal-crashsafe-{uuid.uuid4()}"
        print("CHECKPOINT 1 — Temporal server + one worker + Crashsafe mock tool", flush=True)
        print(f"Temporal SQLite: {STATE / 'temporal.db'}")
        print(f"tool SQLite: {STATE / 'tools.db'}")
        print(f"workflow: {workflow_id}")
        print(f"stable charge key: {workflow_id}:0:charge\n")

        worker = subprocess.Popen(
            [sys.executable, "-m", "experiments.temporal_compare.worker"],
            env=environment,
        )
        workers.append(worker)
        await asyncio.sleep(0.5)
        handle = await client.start_workflow(
            OrderWorkflow.run,
            OrderInput(
                customer_id="temporal-demo-customer",
                amount_cents=4200,
                email="temporal@example.com",
            ),
            id=workflow_id,
            task_queue=TASK_QUEUE,
        )
        await wait_for_file(commit_signal)

        print("\nCHECKPOINT 2 — tool committed; kill Temporal worker before completion")
        print(f"tool ledger before kill: {await ledger(tool_url)}")
        print(f"SIGKILL Temporal worker PID {worker.pid}")
        os.kill(worker.pid, signal.SIGKILL)
        if worker.wait(timeout=5) != -signal.SIGKILL:
            raise RuntimeError("Temporal worker did not exit from SIGKILL")
        before = await history_summary(handle)
        print("Temporal history before recovery:")
        print(json.dumps(before, indent=2))
        if any(event["type"] == "ACTIVITY_TASK_COMPLETED" for event in before):
            raise RuntimeError("activity unexpectedly completed before worker kill")

        print("\nCHECKPOINT 3 — restart one worker and let Temporal retry")
        replacement = subprocess.Popen(
            [sys.executable, "-m", "experiments.temporal_compare.worker"],
            env=environment,
        )
        workers.append(replacement)
        results = await asyncio.wait_for(handle.result(), timeout=20)
        after = await history_summary(handle)
        print("Temporal history after recovery:")
        print(json.dumps(after, indent=2))

        final_ledger = await ledger(tool_url)
        charge_results = [result for result in results if result["operation"] == "charge"]
        print("\nCHECKPOINT 4 — compare guarantees")
        print(f"charge result deduplicated: {charge_results[0]['deduplicated']}")
        print(f"final durable ledger: {final_ledger}")
        if (
            final_ledger != {"charges": 1, "provisions": 1, "notifications": 1}
            or len(charge_results) != 1
            or not charge_results[0]["deduplicated"]
        ):
            raise RuntimeError("Temporal comparison invariant failed")
        print(
            "PASS — Temporal retried the Activity after worker loss; "
            "the shared idempotency key kept the charge effect at one."
        )
    finally:
        for worker in workers:
            stop(worker)
        stop(tool)
        stop(temporal_server)


if __name__ == "__main__":
    asyncio.run(run())
