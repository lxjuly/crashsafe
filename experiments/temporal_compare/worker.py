from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from experiments.temporal_compare.workflow import TASK_QUEUE, OrderWorkflow, ToolCall


@activity.defn(name="invoke_mock_tool")
async def invoke_mock_tool(call: ToolCall) -> dict[str, Any]:
    attempt = activity.info().attempt
    headers = {"Idempotency-Key": call.operation_key}
    if call.operation == "charge" and attempt == 1:
        headers["X-Crashsafe-Delay-After-Commit"] = "true"
    print(
        f"TEMPORAL ACTIVITY operation={call.operation} attempt={attempt} "
        f"key={call.operation_key}",
        flush=True,
    )
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            f"{os.environ['CRASHSAFE_TOOL_URL']}/tools/{call.operation}",
            json=call.request,
            headers=headers,
        )
    response.raise_for_status()
    result: dict[str, Any] = response.json()
    print(
        f"TEMPORAL ACTIVITY RESULT operation={call.operation} attempt={attempt} "
        f"deduplicated={result['deduplicated']}",
        flush=True,
    )
    return result


async def run() -> None:
    client = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=[invoke_mock_tool],
        max_cached_workflows=0,
        max_concurrent_workflow_tasks=1,
        max_concurrent_activities=1,
    )
    print(f"TEMPORAL WORKER ready pid={os.getpid()}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run())
