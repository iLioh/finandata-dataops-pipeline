from __future__ import annotations

from copy import deepcopy

import pytest

from finandata.tasks.ingestion import (
    extraer_api_bancaria,
    extraer_atm_por_sucursal,
    extraer_core_ach_cdc,
)
from finandata.tasks.quality import (
    construir_metricas,
    evaluar_quality_gate_1,
    evaluar_registros,
    validar_data_contract_record,
)


def valid_record(transaction_id: str = "TX-001") -> dict[str, object]:
    return {
        "transaction_id": transaction_id,
        "iban": "GB82WEST12345698765432",
        "amount": "100.00",
        "currency": "PEN",
        "transaction_date": "2026-08-01T08:00:00Z",
        "branch_id": "LIM-001",
        "channel": "ATM",
        "schema_version": "1.0",
    }


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("amount", "-0.01", "AMOUNT_NEGATIVE"),
        ("transaction_id", None, "TRANSACTION_ID_REQUIRED"),
        ("currency", "EUR", "CURRENCY_NOT_IN_REFERENCE_DATA"),
        ("branch_id", "CUS-999", "BRANCH_NOT_IN_REFERENCE_DATA"),
        ("channel", "WEB", "CHANNEL_NOT_IN_REFERENCE_DATA"),
    ],
)
def test_invalid_quality_values_are_rejected(field: str, value: object, reason: str) -> None:
    record = valid_record()
    record[field] = value
    valid, rejected, _ = evaluar_registros([record])
    assert valid == []
    assert reason in rejected[0]["rejection_reasons"]


def test_duplicate_transaction_is_detected() -> None:
    records = [valid_record(), deepcopy(valid_record())]
    valid, rejected, counters = evaluar_registros(records)
    assert len(valid) == 1
    assert len(rejected) == 1
    assert counters["duplicate_count"] == 1
    assert "TRANSACTION_ID_DUPLICATE" in rejected[0]["rejection_reasons"]


def test_reject_rate_is_calculated() -> None:
    valid_result = {
        "input_records": 100,
        "record_count": 85,
        "complete_records": 100,
        "batch_id": "batch",
        "flow_run_id": "flow",
        "scenario": "incident_15_percent",
    }
    quarantine_result = {
        "record_count": 15,
        "duplicate_count": 0,
    }
    assert construir_metricas(valid_result, quarantine_result)["reject_rate"] == 0.15


def test_data_contract_requires_supported_version_and_structural_types() -> None:
    unsupported = valid_record()
    unsupported["schema_version"] = "2.0"
    assert "unsupported_schema_version=2.0;expected=1.0" in validar_data_contract_record(
        unsupported
    )

    malformed = valid_record()
    malformed.update({"amount": "not-a-number", "transaction_date": "29/08/2026"})
    errors = validar_data_contract_record(malformed)
    assert "amount_not_decimal_convertible" in errors
    assert "transaction_date_not_iso8601" in errors

    missing = valid_record()
    missing.pop("currency")
    assert "missing_or_empty=currency" in validar_data_contract_record(missing)


def test_quality_gate_1_pass_and_fail() -> None:
    policy = {
        "max_reject_rate": 0.05,
        "min_completeness": 0.95,
        "max_duplicate_rate": 0.0,
    }
    passing = {"reject_rate": 0.0, "completeness": 1.0, "duplicate_rate": 0.0}
    failing = {**passing, "reject_rate": 0.15}
    assert evaluar_quality_gate_1(passing, policy) == (True, [])
    assert evaluar_quality_gate_1(failing, policy) == (False, ["reject_rate"])


def test_incident_15_percent_has_exact_evidence() -> None:
    records = []
    for branch in ("LIM-001", "LIM-002", "LIM-003"):
        records.extend(extraer_atm_por_sucursal.fn(branch, "incident_15_percent"))
    records.extend(extraer_api_bancaria.fn("incident_15_percent"))
    records.extend(extraer_core_ach_cdc.fn("incident_15_percent"))
    valid, rejected, counters = evaluar_registros(records)
    valid_result = {
        "input_records": len(records),
        "record_count": len(valid),
        "complete_records": counters["complete_records"],
        "batch_id": "incident-test",
        "flow_run_id": "flow-test",
        "scenario": "incident_15_percent",
    }
    quarantine_result = {
        "record_count": len(rejected),
        "duplicate_count": counters["duplicate_count"],
    }
    metrics = construir_metricas(valid_result, quarantine_result)
    passed, _ = evaluar_quality_gate_1(
        metrics,
        {"max_reject_rate": 0.05, "min_completeness": 0.95, "max_duplicate_rate": 0.0},
    )
    assert metrics["input_records"] == 100
    assert metrics["valid_records"] == 85
    assert metrics["rejected_records"] == 15
    assert metrics["reject_rate"] == 0.15
    assert passed is False
