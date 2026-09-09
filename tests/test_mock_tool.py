from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from crashsafe.mock_tool import ChargeRequest, IdempotencyConflictError, ToolStore, create_app
from crashsafe.models import StepName


def test_side_effect_and_idempotency_result_are_durable(tmp_path: Path) -> None:
    path = tmp_path / "tool.db"
    store = ToolStore(path)
    request = ChargeRequest(customer_id="customer-1", amount_cents=2500)

    first, created = store.apply(StepName.CHARGE, "stable-key", request)
    assert created is True

    reopened = ToolStore(path)
    second, created = reopened.apply(StepName.CHARGE, "stable-key", request)
    assert created is False
    assert second.reference_id == first.reference_id
    assert second.deduplicated is True
    assert reopened.summary().charges == 1


def test_idempotency_key_rejects_a_different_payload(tmp_path: Path) -> None:
    store = ToolStore(tmp_path / "tool.db")
    store.apply(
        StepName.CHARGE,
        "stable-key",
        ChargeRequest(customer_id="customer-1", amount_cents=2500),
    )
    with pytest.raises(IdempotencyConflictError):
        store.apply(
            StepName.CHARGE,
            "stable-key",
            ChargeRequest(customer_id="customer-1", amount_cents=9999),
        )


def test_deterministic_429_injection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CRASHSAFE_FAIL_FIRST_N", "1")
    monkeypatch.setenv("CRASHSAFE_FLAKY_RATE", "0")
    monkeypatch.setenv("CRASHSAFE_RETRY_AFTER", "0.25")
    client = TestClient(create_app(ToolStore(tmp_path / "tool.db")))
    headers = {"Idempotency-Key": "one"}
    request = {"customer_id": "customer-1", "amount_cents": 2500}

    throttled = client.post("/tools/charge", json=request, headers=headers)
    completed = client.post("/tools/charge", json=request, headers=headers)

    assert throttled.status_code == 429
    assert throttled.headers["Retry-After"] == "0.25"
    assert completed.status_code == 200
    assert client.get("/ledger").json()["charges"] == 1


def test_deterministic_429_can_target_an_operation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CRASHSAFE_FAIL_FIRST_OPERATION", "provision")
    monkeypatch.setenv("CRASHSAFE_FLAKY_RATE", "0")
    client = TestClient(create_app(ToolStore(tmp_path / "tool.db")))

    charge = client.post(
        "/tools/charge",
        json={"customer_id": "customer-1", "amount_cents": 2500},
        headers={"Idempotency-Key": "charge"},
    )
    first_provision = client.post(
        "/tools/provision",
        json={"customer_id": "customer-1", "plan": "trial"},
        headers={"Idempotency-Key": "provision"},
    )
    second_provision = client.post(
        "/tools/provision",
        json={"customer_id": "customer-1", "plan": "trial"},
        headers={"Idempotency-Key": "provision"},
    )

    assert charge.status_code == 200
    assert first_provision.status_code == 429
    assert second_provision.status_code == 200
    assert client.get("/ledger").json() == {
        "charges": 1,
        "provisions": 1,
        "notifications": 0,
    }
