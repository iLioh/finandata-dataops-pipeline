from __future__ import annotations

from decimal import Decimal

from finandata.config import Settings
from finandata.tasks.warehouse import Warehouse, evaluar_quality_gate_2, evaluar_reconciliacion


def test_reconciliation_passes_for_matching_totals() -> None:
    evidence = evaluar_reconciliacion(10, Decimal("125.50"), 10, Decimal("125.50"))
    assert evidence["passed"] is True
    assert evidence["count_diff"] == 0
    assert evidence["amount_diff"] == Decimal("0")


def test_reconciliation_detects_count_difference() -> None:
    evidence = evaluar_reconciliacion(10, Decimal("125.50"), 9, Decimal("125.50"))
    assert evidence["passed"] is False
    assert evidence["count_diff"] == -1


def test_reconciliation_detects_amount_difference() -> None:
    evidence = evaluar_reconciliacion(10, Decimal("125.50"), 10, Decimal("120.00"))
    assert evidence["passed"] is False
    assert evidence["amount_diff"] == Decimal("-5.50")


def test_quality_gate_2_pass_and_fail() -> None:
    passing = {"passed": True, "post_load_passed": True}
    failed_post_load = {"passed": True, "post_load_passed": False}
    failed_reconciliation = {"passed": False, "post_load_passed": True}
    assert evaluar_quality_gate_2(passing) is True
    assert evaluar_quality_gate_2(failed_post_load) is False
    assert evaluar_quality_gate_2(failed_reconciliation) is False


def test_local_dwh_upsert_is_idempotent(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DWH_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DWH_PATH", str(tmp_path / "idempotent.db"))
    warehouse = Warehouse(Settings.from_env())
    record = {
        "transaction_id": "IDEMPOTENT-001",
        "transaction_date": "2026-08-01T08:00:00+00:00",
        "branch_id": "LIM-001",
        "channel": "ATM",
        "currency": "PEN",
        "amount": Decimal("100.00"),
        "commission": Decimal("0.50"),
        "debit_amount": Decimal("100.00"),
        "credit_amount": Decimal("100.00"),
        "risk_score": 20,
        "iban_masked": "****5432",
        "source_system": "ATM",
        "batch_id": "idempotent-batch",
        "flow_run_id": "flow",
        "record_hash": "hash",
    }
    warehouse.upsert_transactions([record])
    warehouse.upsert_transactions([record])
    assert warehouse.batch_snapshot("idempotent-batch")["loaded_records"] == 1
