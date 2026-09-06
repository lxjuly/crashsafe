from __future__ import annotations

from pathlib import Path

import pytest

from crashsafe.mock_tool import ChargeRequest, IdempotencyConflictError, ToolStore
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
