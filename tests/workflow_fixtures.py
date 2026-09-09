from __future__ import annotations

from crashsafe.models import WorkflowDefinition


def paid_workflow_definition(customer: str = "customer-1") -> WorkflowDefinition:
    return WorkflowDefinition.model_validate(
        {
            "name": f"paid-{customer}",
            "steps": [
                {
                    "id": "charge",
                    "operation": "charge",
                    "depends_on": [],
                    "request": {"customer_id": customer, "amount_cents": 2500},
                },
                {
                    "id": "provision",
                    "operation": "provision",
                    "depends_on": ["charge"],
                    "request": {"customer_id": customer, "plan": "standard"},
                },
                {
                    "id": "notify",
                    "operation": "notify",
                    "depends_on": ["provision"],
                    "request": {
                        "customer_id": customer,
                        "email": f"{customer}@example.com",
                        "message": "Your account is ready.",
                    },
                },
            ],
        }
    )


def branched_workflow_definition(customer: str = "branch") -> WorkflowDefinition:
    value = paid_workflow_definition(customer).model_dump(mode="json")
    value["name"] = "branched"
    value["steps"][2]["depends_on"] = ["charge"]
    return WorkflowDefinition.model_validate(value)
