"""Deterministic Silver and Gold transformations."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from prefect import task

from finandata.observability import registrar_telemetria
from finandata.storage import get_storage


MONEY_QUANTUM = Decimal("0.01")
COMMISSION_RATES = {
    "ATM": Decimal("0.005"),
    "MOBILE": Decimal("0.002"),
    "ACH": Decimal("0.001"),
}


def normalizar_silver(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in records:
        transaction_id = str(source["transaction_id"]).strip().upper()
        if transaction_id in seen:
            continue
        seen.add(transaction_id)
        date_value = datetime.fromisoformat(str(source["transaction_date"]).replace("Z", "+00:00"))
        if date_value.tzinfo is None:
            date_value = date_value.replace(tzinfo=UTC)
        normalized.append(
            {
                **source,
                "transaction_id": transaction_id,
                "iban": str(source["iban"]).replace(" ", "").upper(),
                "amount": Decimal(str(source["amount"])).quantize(MONEY_QUANTUM),
                "currency": str(source["currency"]).strip().upper(),
                "transaction_date": date_value.astimezone(UTC).isoformat(),
                "branch_id": str(source["branch_id"]).strip().upper(),
                "channel": str(source["channel"]).strip().upper(),
            }
        )
    return normalized


def calcular_comision(amount: Any, channel: str) -> Decimal:
    rate = COMMISSION_RATES[str(channel).upper()]
    return (Decimal(str(amount)) * rate).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def calcular_risk_score(amount: Any, channel: str) -> int:
    value = Decimal(str(amount))
    score = 20 if value < 1000 else 50 if value < 5000 else 80
    if str(channel).upper() == "ACH":
        score += 10
    return min(score, 100)


def enmascarar_iban(iban: Any) -> str:
    clean = str(iban).replace(" ", "")
    return f"****{clean[-4:]}"


def enriquecer_gold(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gold_records: list[dict[str, Any]] = []
    for source in records:
        record = dict(source)
        iban = record.pop("iban")
        record.pop("raw_payload", None)
        amount = Decimal(str(record["amount"])).quantize(MONEY_QUANTUM)
        record.update(
            {
                "amount": amount,
                "commission": calcular_comision(amount, str(record["channel"])),
                "risk_score": calcular_risk_score(amount, str(record["channel"])),
                "iban_masked": enmascarar_iban(iban),
                "debit_amount": amount,
                "credit_amount": amount,
            }
        )
        gold_records.append(record)
    return gold_records


@task(name="transformar_silver", retries=0)
def transformar_silver(valid_result: dict[str, Any], gate1: dict[str, Any]) -> dict[str, Any]:
    if not gate1["passed"]:
        raise RuntimeError("Silver no está autorizado por QG1")
    started = time.perf_counter()
    records = normalizar_silver(valid_result["records"])
    return {
        "records": records,
        "batch_id": valid_result["batch_id"],
        "flow_run_id": valid_result["flow_run_id"],
        "scenario": valid_result["scenario"],
        "metrics": gate1["metrics"],
        "task_duration": round(time.perf_counter() - started, 6),
    }


@task(name="persistir_silver", retries=3, retry_delay_seconds=[1, 2, 4])
def persistir_silver(silver_result: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    artifact = get_storage().write_records(
        "silver",
        silver_result["batch_id"],
        "certified-transactions",
        silver_result["records"],
        {
            "flow_run_id": silver_result["flow_run_id"],
            "scenario": silver_result["scenario"],
            "metrics": silver_result["metrics"],
        },
    )
    registrar_telemetria(
        "SILVER_LOAD",
        batch_id=silver_result["batch_id"],
        valid_records=len(silver_result["records"]),
        task_duration=round(time.perf_counter() - started, 6),
        retry_count=0,
    )
    return artifact


@task(name="transformar_enriquecer", retries=0)
def transformar_enriquecer(silver_artifact: dict[str, Any]) -> dict[str, Any]:
    records = get_storage().read_records(silver_artifact)
    return {
        "records": enriquecer_gold(records),
        "batch_id": silver_artifact["batch_id"],
        **silver_artifact["metadata"],
    }


@task(name="persistir_gold", retries=3, retry_delay_seconds=[1, 2, 4])
def persistir_gold(gold_result: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    records = gold_result["records"]
    expected_amount = sum((Decimal(str(row["amount"])) for row in records), Decimal("0"))
    artifact = get_storage().write_records(
        "gold",
        gold_result["batch_id"],
        "financial-transactions",
        records,
        {
            "flow_run_id": gold_result["flow_run_id"],
            "scenario": gold_result["scenario"],
            "metrics": gold_result["metrics"],
            "expected_count": len(records),
            "expected_amount": str(expected_amount),
        },
    )
    registrar_telemetria(
        "GOLD_LOAD",
        batch_id=gold_result["batch_id"],
        valid_records=len(records),
        task_duration=round(time.perf_counter() - started, 6),
        retry_count=0,
    )
    return artifact
