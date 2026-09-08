from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from workflow_fixtures import paid_workflow

from crashsafe.api import create_app
from crashsafe.storage import SQLiteStorage


def test_workflow_history_and_timeline_endpoints(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    client = TestClient(create_app(store))
    created = client.post("/workflows", json=paid_workflow("api-test").model_dump(mode="json"))
    assert created.status_code == 201
    workflow = created.json()
    workflow_id = workflow["id"]
    assert workflow["name"] == "paid-api-test"
    assert workflow["steps"][1]["depends_on"] == ["charge"]
    assert workflow["steps"][0]["operation_key"] == f"{workflow_id}:charge"

    history = client.get(f"/workflows/{workflow_id}/events")
    assert history.status_code == 200
    assert [event["event_type"] for event in history.json()] == ["WorkflowCreated"]

    timeline = client.get(f"/workflows/{workflow_id}/timeline")
    assert timeline.status_code == 200
    assert "audit_consistent" not in timeline.json()["summary"]
    assert timeline.json()["entries"][0]["event_type"] == "WorkflowCreated"

    assert client.get(f"/workflows/{workflow_id}/audit").status_code == 404
    assert client.get("/workflows/missing/events").status_code == 404
    assert client.get("/workflows/missing/timeline").status_code == 404


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["steps"].append(value["steps"][0]),
        lambda value: value["steps"][1].update(depends_on=["missing"]),
        lambda value: value["steps"][0].update(depends_on=["notify"]),
        lambda value: value["steps"][0].update(operation="unknown"),
        lambda value: value["steps"][0]["request"].update(amount_cents=0),
        lambda value: value.update(unexpected=True),
    ],
)
def test_invalid_workflow_definitions_return_422(settings: object, mutate: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    client = TestClient(create_app(store))
    value = paid_workflow().model_dump(mode="json")
    mutate(value)  # type: ignore[operator]
    assert client.post("/workflows", json=value).status_code == 422
