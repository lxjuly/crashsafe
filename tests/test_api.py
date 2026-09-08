from __future__ import annotations

from fastapi.testclient import TestClient

from crashsafe.api import create_app
from crashsafe.storage import SQLiteStorage


def test_history_and_audit_endpoints(settings: object) -> None:
    store = SQLiteStorage(settings.engine_db)  # type: ignore[attr-defined]
    client = TestClient(create_app(store))
    created = client.post(
        "/workflows",
        json={
            "customer_id": "api-test",
            "amount_cents": 4200,
            "email": "api@example.com",
        },
    )
    assert created.status_code == 201
    workflow_id = created.json()["id"]

    history = client.get(f"/workflows/{workflow_id}/events")
    assert history.status_code == 200
    assert [event["event_type"] for event in history.json()] == ["WorkflowCreated"]

    audit = client.get(f"/workflows/{workflow_id}/audit")
    assert audit.status_code == 200
    assert audit.json()["consistent"] is True

    timeline = client.get(f"/workflows/{workflow_id}/timeline")
    assert timeline.status_code == 200
    assert timeline.json()["summary"]["audit_consistent"] is True
    assert timeline.json()["entries"][0]["event_type"] == "WorkflowCreated"

    assert client.get("/workflows/missing/events").status_code == 404
    assert client.get("/workflows/missing/audit").status_code == 404
    assert client.get("/workflows/missing/timeline").status_code == 404
