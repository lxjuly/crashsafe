"""FastAPI validation and workflow-run read-surface tests."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from workflow_fixtures import paid_workflow_definition

from crashsafe.api import create_app
from crashsafe.storage import SQLiteStorage


def test_workflow_run_collection_history_and_timeline_endpoints(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    client = TestClient(create_app(store))
    created = client.post(
        "/workflow_runs", json=paid_workflow_definition("api-test").model_dump(mode="json")
    )
    assert created.status_code == 201
    run = created.json()
    run_id = run["run_id"]
    assert run["name"] == "paid-api-test"
    assert run["steps"][1]["depends_on"] == ["charge"]
    assert run["steps"][0]["operation_key"] == f"{run_id}:charge"

    collection = client.get("/workflow_runs")
    assert collection.status_code == 200
    assert [item["run_id"] for item in collection.json()] == [run_id]

    history = client.get(f"/workflow_runs/{run_id}/events")
    assert history.status_code == 200
    assert [event["event_type"] for event in history.json()] == ["WorkflowRunCreated"]

    timeline = client.get(f"/workflow_runs/{run_id}/timeline")
    assert timeline.status_code == 200
    assert "audit_consistent" not in timeline.json()["summary"]
    assert timeline.json()["entries"][0]["event_type"] == "WorkflowRunCreated"

    assert client.get(f"/workflow_runs/{run_id}/audit").status_code == 404
    assert client.get("/workflows").status_code == 404
    assert client.get("/workflow_runs/missing/events").status_code == 404
    assert client.get("/workflow_runs/missing/timeline").status_code == 404


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
    value = paid_workflow_definition().model_dump(mode="json")
    mutate(value)  # type: ignore[operator]
    assert client.post("/workflow_runs", json=value).status_code == 422
