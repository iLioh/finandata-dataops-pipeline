"""Data Contract, Data Quality, metrics and Quality Gate 1."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from prefect import task

from finandata.alerts import emitir_alerta
from finandata.config import BRANCHES, CHANNELS, CURRENCIES, load_quality_policy
from finandata.observability import registrar_telemetria
from finandata.storage import get_storage


CONTRACT_FIELDS = frozenset(
    {
        "transaction_id",
        "iban",
        "amount",
        "currency",
        "transaction_date",
        "branch_id",
        "channel",
        "schema_version",
    }
)
SUPPORTED_SCHEMA_VERSION = "1.0"
CONTRACT_TEXT_FIELDS = (
    "transaction_id",
    "iban",
    "currency",
    "transaction_date",
    "branch_id",
    "channel",
    "schema_version",
)
QUALITY_REQUIRED_FIELDS = (
    "transaction_id",
    "amount",
    "iban",
    "transaction_date",
    "currency",
    "branch_id",
    "channel",
)


def validar_iban(iban: Any) -> bool:
    clean = re.sub(r"\s+", "", str(iban or "")).upper()
    if not re.fullmatch(r"[A-Z]{2}\d{2}[A-Z0-9]{11,30}", clean):
        return False
    rearranged = clean[4:] + clean[:4]
    numeric = "".join(str(ord(char) - 55) if char.isalpha() else char for char in rearranged)
    return int(numeric) % 97 == 1


def _valid_date(value: Any) -> bool:
    if value in (None, ""):
        return False
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    return True


def validar_data_contract_record(record: dict[str, Any]) -> list[str]:
    """Validate required fields, structural types and the supported contract version."""
    errors: list[str] = []
    missing = sorted(
        field for field in CONTRACT_FIELDS if field not in record or record[field] in (None, "")
    )
    if missing:
        errors.append(f"missing_or_empty={','.join(missing)}")
    for field in CONTRACT_TEXT_FIELDS:
        if field in record and record[field] not in (None, "") and not isinstance(record[field], str):
            errors.append(f"{field}_type_must_be_string")
    if "amount" in record and record.get("amount") not in (None, ""):
        try:
            amount = Decimal(str(record["amount"]))
            if not amount.is_finite():
                errors.append("amount_must_be_finite")
        except (InvalidOperation, TypeError, ValueError):
            errors.append("amount_not_decimal_convertible")
    if record.get("transaction_date") not in (None, "") and not _valid_date(
        record["transaction_date"]
    ):
        errors.append("transaction_date_not_iso8601")
    if record.get("schema_version") not in (None, "", SUPPORTED_SCHEMA_VERSION):
        errors.append(
            f"unsupported_schema_version={record['schema_version']};expected={SUPPORTED_SCHEMA_VERSION}"
        )
    return errors


def evaluar_registros(
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    duplicate_count = 0
    complete_records = 0
    for original in records:
        record = dict(original)
        reasons: list[str] = []
        transaction_id = record.get("transaction_id")
        if transaction_id in (None, ""):
            reasons.append("TRANSACTION_ID_REQUIRED")
        elif str(transaction_id) in seen_ids:
            reasons.append("TRANSACTION_ID_DUPLICATE")
            duplicate_count += 1
        else:
            seen_ids.add(str(transaction_id))
        try:
            if Decimal(str(record.get("amount"))) < 0:
                reasons.append("AMOUNT_NEGATIVE")
        except (InvalidOperation, TypeError, ValueError):
            reasons.append("AMOUNT_INVALID")
        if not validar_iban(record.get("iban")):
            reasons.append("IBAN_INVALID")
        if not _valid_date(record.get("transaction_date")):
            reasons.append("TRANSACTION_DATE_INVALID")
        if str(record.get("currency", "")).upper() not in CURRENCIES:
            reasons.append("CURRENCY_NOT_IN_REFERENCE_DATA")
        if str(record.get("branch_id", "")).upper() not in BRANCHES:
            reasons.append("BRANCH_NOT_IN_REFERENCE_DATA")
        if str(record.get("channel", "")).upper() not in CHANNELS:
            reasons.append("CHANNEL_NOT_IN_REFERENCE_DATA")
        if all(record.get(field) not in (None, "") for field in QUALITY_REQUIRED_FIELDS):
            complete_records += 1
        if reasons:
            record["rejection_reasons"] = reasons
            rejected.append(record)
        else:
            valid.append(record)
    return valid, rejected, {
        "duplicate_count": duplicate_count,
        "complete_records": complete_records,
    }


def _record_blocked_batch(**values: Any) -> None:
    try:
        from finandata.tasks.warehouse import record_batch_control

        record_batch_control(**values)
    except Exception:
        # A metadata write must not hide the original quality decision.
        return


@task(name="validar_esquema", retries=0)
def validar_esquema(bronze_artifact: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    records = get_storage().read_records(bronze_artifact)
    errors: list[str] = []
    for index, record in enumerate(records):
        try:
            raw_record = json.loads(str(record.get("raw_payload", "{}")))
        except (json.JSONDecodeError, TypeError):
            errors.append(f"record[{index}] raw_payload_invalid_json")
            continue
        if not isinstance(raw_record, dict):
            errors.append(f"record[{index}] raw_payload_must_be_object")
            continue
        errors.extend(
            f"record[{index}] {error}" for error in validar_data_contract_record(raw_record)
        )
    metadata = bronze_artifact["metadata"]
    result = {
        "passed": not errors,
        "bronze_artifact": bronze_artifact,
        "errors": errors,
        "batch_id": bronze_artifact["batch_id"],
        "flow_run_id": metadata["flow_run_id"],
        "scenario": metadata["scenario"],
        "input_records": len(records),
    }
    registrar_telemetria(
        "SCHEMA_VALIDATION",
        batch_id=result["batch_id"],
        input_records=len(records),
        schema_status="PASS" if result["passed"] else "FAIL",
        task_duration=round(time.perf_counter() - started, 6),
    )
    return result


@task(name="enviar_schema_quarantine", retries=0)
def enviar_schema_quarantine(contract_result: dict[str, Any]) -> dict[str, Any]:
    if contract_result["passed"]:
        raise RuntimeError("No se envía a Schema Quarantine un contrato válido")
    records = get_storage().read_records(contract_result["bronze_artifact"])
    quarantine = get_storage().write_records(
        "schema-quarantine",
        contract_result["batch_id"],
        "invalid-contract",
        records,
        {
            "errors": contract_result["errors"],
            "flow_run_id": contract_result["flow_run_id"],
        },
    )
    registrar_telemetria(
        "SCHEMA_QUARANTINE",
        batch_id=contract_result["batch_id"],
        input_records=contract_result["input_records"],
        error_count=len(contract_result["errors"]),
    )
    return {
        "failure_type": "SCHEMA_VALIDATION_FAILED",
        "message": "El Data Contract falló; el RAW permanece en Bronze y se bloqueó el lote.",
        "pipeline_status": "BLOCKED_SCHEMA",
        "batch_id": contract_result["batch_id"],
        "flow_run_id": contract_result["flow_run_id"],
        "scenario": contract_result["scenario"],
        "input_records": contract_result["input_records"],
        "errors": contract_result["errors"],
        "metric_value": len(contract_result["errors"]),
        "threshold": 0,
        "schema_quarantine": quarantine,
    }


@task(name="validar_data_quality", retries=0)
def validar_data_quality(contract_result: dict[str, Any]) -> dict[str, Any]:
    if not contract_result["passed"]:
        raise RuntimeError("Data Quality no puede ejecutarse después de un Data Contract FAIL")
    started = time.perf_counter()
    records = get_storage().read_records(contract_result["bronze_artifact"])
    valid, rejected, counters = evaluar_registros(records)
    registrar_telemetria(
        "DATA_QUALITY",
        batch_id=contract_result["batch_id"],
        input_records=len(records),
        valid_records=len(valid),
        rejected_records=len(rejected),
        task_duration=round(time.perf_counter() - started, 6),
    )
    return {
        "valid_records": valid,
        "rejected_records": rejected,
        "input_records": len(records),
        **counters,
        "batch_id": contract_result["batch_id"],
        "flow_run_id": contract_result["flow_run_id"],
        "scenario": contract_result["scenario"],
    }


@task(name="obtener_registros_validos", retries=0)
def obtener_registros_validos(dq_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "records": dq_result["valid_records"],
        "record_count": len(dq_result["valid_records"]),
        "input_records": dq_result["input_records"],
        "complete_records": dq_result["complete_records"],
        "batch_id": dq_result["batch_id"],
        "flow_run_id": dq_result["flow_run_id"],
        "scenario": dq_result["scenario"],
    }


@task(name="guardar_data_quarantine", retries=0)
def guardar_data_quarantine(dq_result: dict[str, Any]) -> dict[str, Any]:
    quarantine = get_storage().write_records(
        "data-quarantine",
        dq_result["batch_id"],
        "rejected-records",
        dq_result["rejected_records"],
        {"flow_run_id": dq_result["flow_run_id"]},
    )
    registrar_telemetria(
        "DATA_QUARANTINE",
        batch_id=dq_result["batch_id"],
        rejected_records=len(dq_result["rejected_records"]),
    )
    return {
        "artifact": quarantine,
        "record_count": len(dq_result["rejected_records"]),
        "duplicate_count": dq_result["duplicate_count"],
        "batch_id": dq_result["batch_id"],
        "flow_run_id": dq_result["flow_run_id"],
        "scenario": dq_result["scenario"],
    }


def construir_metricas(
    valid_result: dict[str, Any], quarantine_result: dict[str, Any]
) -> dict[str, Any]:
    input_records = int(valid_result["input_records"])
    valid_records = int(valid_result["record_count"])
    rejected_records = int(quarantine_result["record_count"])
    divisor = input_records or 1
    return {
        "input_records": input_records,
        "valid_records": valid_records,
        "rejected_records": rejected_records,
        "reject_rate": rejected_records / divisor,
        "completeness": int(valid_result["complete_records"]) / divisor,
        "duplicate_rate": int(quarantine_result["duplicate_count"]) / divisor,
        "batch_id": valid_result["batch_id"],
        "flow_run_id": valid_result["flow_run_id"],
        "scenario": valid_result["scenario"],
    }


@task(name="calcular_quality_metrics", retries=0)
def calcular_quality_metrics(
    valid_result: dict[str, Any], quarantine_result: dict[str, Any]
) -> dict[str, Any]:
    metrics = construir_metricas(valid_result, quarantine_result)
    registrar_telemetria("QUALITY_METRICS", **metrics)
    return metrics


def evaluar_quality_gate_1(
    metrics: dict[str, Any], policy: dict[str, float]
) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if metrics["reject_rate"] > policy["max_reject_rate"]:
        failures.append("reject_rate")
    if metrics["completeness"] < policy["min_completeness"]:
        failures.append("completeness")
    if metrics["duplicate_rate"] > policy["max_duplicate_rate"]:
        failures.append("duplicate_rate")
    return not failures, failures


@task(name="quality_gate_1", retries=0)
def quality_gate_1(metrics: dict[str, Any]) -> dict[str, Any]:
    policy = load_quality_policy()
    passed, failures = evaluar_quality_gate_1(metrics, policy)
    status = "PASS" if passed else "FAIL"
    result = {
        "passed": passed,
        "status": status,
        "failures": failures,
        "policy": policy,
        "metrics": metrics,
        "batch_id": metrics["batch_id"],
        "flow_run_id": metrics["flow_run_id"],
        "scenario": metrics["scenario"],
    }
    registrar_telemetria("QUALITY_GATE_1", qg1_status=status, **metrics)
    if not passed:
        result.update(
            {
                "failure_type": "QUALITY_GATE_1_FAILED",
                "message": "QG1 bloqueó la promoción hacia Silver.",
                "pipeline_status": "FAILED_QUALITY",
                "metric_value": {name: metrics[name] for name in failures},
                "threshold": policy,
            }
        )
    return result


@task(name="alertar_y_detener", retries=0)
def alertar_y_detener(failure_result: dict[str, Any]) -> dict[str, Any]:
    failure_type = failure_result["failure_type"]
    emitir_alerta(
        failure_type,
        failure_result["message"],
        batch_id=failure_result["batch_id"],
        flow_run_id=failure_result["flow_run_id"],
        task_name="alertar_y_detener",
        metric_value=failure_result["metric_value"],
        threshold=failure_result["threshold"],
    )
    if failure_type == "SCHEMA_VALIDATION_FAILED":
        _record_blocked_batch(
            batch_id=failure_result["batch_id"],
            flow_run_id=failure_result["flow_run_id"],
            input_records=failure_result["input_records"],
            pipeline_status="BLOCKED_SCHEMA",
            qg1_status="NOT_EVALUATED",
            qg2_status="NOT_EVALUATED",
        )
    elif failure_type == "QUALITY_GATE_1_FAILED":
        metrics = failure_result["metrics"]
        _record_blocked_batch(
            batch_id=metrics["batch_id"],
            flow_run_id=metrics["flow_run_id"],
            input_records=metrics["input_records"],
            valid_records=metrics["valid_records"],
            rejected_records=metrics["rejected_records"],
            reject_rate=metrics["reject_rate"],
            qg1_status="FAIL",
            qg2_status="NOT_EVALUATED",
            pipeline_status="FAILED_QUALITY",
        )
    else:
        raise ValueError(f"Ruta de fallo no soportada: {failure_type}")
    return {
        "batch_id": failure_result["batch_id"],
        "pipeline_status": failure_result["pipeline_status"],
        "stopped": True,
        "failure_type": failure_type,
    }
