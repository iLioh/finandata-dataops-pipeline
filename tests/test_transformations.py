from __future__ import annotations

from decimal import Decimal

from finandata.tasks.transformation import (
    calcular_comision,
    calcular_risk_score,
    enriquecer_gold,
    normalizar_silver,
)


def record(transaction_id: str = " tx-1 ") -> dict[str, object]:
    return {
        "transaction_id": transaction_id,
        "iban": "GB82 WEST 1234 5698 7654 32",
        "amount": "1000",
        "currency": " pen ",
        "transaction_date": "2026-08-01T08:00:00Z",
        "branch_id": "lim-001",
        "channel": "atm",
        "source_system": "ATM",
        "batch_id": "batch",
        "flow_run_id": "flow",
        "record_hash": "hash",
    }


def test_silver_normalizes_and_deduplicates() -> None:
    normalized = normalizar_silver([record(), record("TX-1")])
    assert len(normalized) == 1
    assert normalized[0]["transaction_id"] == "TX-1"
    assert normalized[0]["currency"] == "PEN"
    assert normalized[0]["branch_id"] == "LIM-001"
    assert normalized[0]["amount"] == Decimal("1000.00")


def test_commission_and_demo_risk_score() -> None:
    assert calcular_comision("1000.00", "ATM") == Decimal("5.00")
    assert calcular_comision("1000.00", "MOBILE") == Decimal("2.00")
    assert calcular_risk_score("999.99", "ATM") == 20
    assert calcular_risk_score("1000.00", "ACH") == 60


def test_gold_masks_and_removes_full_iban() -> None:
    gold = enriquecer_gold(normalizar_silver([record()]))[0]
    assert "iban" not in gold
    assert gold["iban_masked"] == "****5432"
    assert "GB82" not in gold["iban_masked"]
    assert gold["debit_amount"] == gold["credit_amount"]

