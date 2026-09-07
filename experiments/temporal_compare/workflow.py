from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

TASK_QUEUE = "crashsafe-temporal-comparison"


@dataclass(frozen=True)
class OrderInput:
    customer_id: str
    amount_cents: int
    email: str


@dataclass(frozen=True)
class ToolCall:
    operation: str
    request: dict[str, Any]
    operation_key: str


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order: OrderInput) -> list[dict[str, Any]]:
        workflow_id = workflow.info().workflow_id
        steps = (
            (
                "charge",
                {
                    "customer_id": order.customer_id,
                    "amount_cents": order.amount_cents,
                },
            ),
            (
                "provision",
                {"customer_id": order.customer_id, "plan": "standard"},
            ),
            (
                "notify",
                {
                    "customer_id": order.customer_id,
                    "email": order.email,
                    "message": "Your account is ready.",
                },
            ),
        )
        results: list[dict[str, Any]] = []
        for position, (operation, request) in enumerate(steps):
            results.append(
                await workflow.execute_activity(
                    "invoke_mock_tool",
                    ToolCall(
                        operation=operation,
                        request=request,
                        operation_key=f"{workflow_id}:{position}:{operation}",
                    ),
                    result_type=dict,
                    start_to_close_timeout=timedelta(seconds=2),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(milliseconds=250),
                        maximum_interval=timedelta(seconds=1),
                        maximum_attempts=5,
                    ),
                )
            )
        return results
