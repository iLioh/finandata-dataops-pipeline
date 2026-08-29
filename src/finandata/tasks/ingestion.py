"""Simulated banking source ingestion and the single Bronze load."""

from __future__ import annotations

import csv
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import task

from finandata.config import PROJECT_ROOT
from finandata.observability import registrar_telemetria
from finandata.storage import get_storage


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as source:
        return [dict(row) for row in csv.DictReader(source)]


def _read_json(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, list):
        raise ValueError(f"La fuente {path} debe contener una lista JSON")
    return [dict(row) for row in value]


@task(name="extraer_atm_por_sucursal", retries=3, retry_delay_seconds=[1, 2, 4])
def extraer_atm_por_sucursal(branch_id: str, scenario: str) -> list[dict[str, Any]]:
    started = time.perf_counter()
    records = _read_csv(PROJECT_ROOT / "data" / "source" / "atm" / f"atm_{branch_id}.csv")
    if scenario == "incident_15_percent" and branch_id == "LIM-001":
        for record in records[:15]:
            record["amount"] = "-1.00"
    for record in records:
        record["_source_system"] = "ATM"
    registrar_telemetria(
        "INGEST_ATM",
        branch_id=branch_id,
        input_records=len(records),
        ingestion_latency=round(time.perf_counter() - started, 6),
        retry_count=0,
    )
    return records


@task(name="extraer_api_bancaria", retries=3, retry_delay_seconds=[1, 2, 4])
def extraer_api_bancaria(scenario: str) -> list[dict[str, Any]]:
    started = time.perf_counter()
    records = _read_json(PROJECT_ROOT / "data" / "source" / "api" / "mobile_transactions.json")
    if scenario == "schema_fail":
        records[0].pop("currency", None)
    for record in records:
        record["_source_system"] = "MOBILE_API"
    registrar_telemetria(
        "INGEST_API",
        input_records=len(records),
        ingestion_latency=round(time.perf_counter() - started, 6),
        retry_count=0,
    )
    return records


@task(name="extraer_core_ach_cdc", retries=3, retry_delay_seconds=[1, 2, 4])
def extraer_core_ach_cdc(scenario: str) -> list[dict[str, Any]]:
    started = time.perf_counter()
    records = _read_json(PROJECT_ROOT / "data" / "source" / "cdc" / "ach_changes.json")
    for record in records:
        record["_source_system"] = "CORE_ACH_CDC"
    registrar_telemetria(
        "INGEST_CDC",
        input_records=len(records),
        ingestion_latency=round(time.perf_counter() - started, 6),
        retry_count=0,
    )
    return records


def _record_hash(raw_record: dict[str, Any]) -> str:
    canonical = json.dumps(raw_record, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@task(name="cargar_bronze", retries=3, retry_delay_seconds=[1, 2, 4])
def cargar_bronze(
    atm_records: list[list[dict[str, Any]]],
    api_records: list[dict[str, Any]],
    cdc_records: list[dict[str, Any]],
    batch_id: str,
    flow_run_id: str,
    scenario: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    ingestion_timestamp = datetime.now(UTC).isoformat()
    consolidated = [record for branch_records in atm_records for record in branch_records]
    consolidated.extend(api_records)
    consolidated.extend(cdc_records)
    bronze_records: list[dict[str, Any]] = []
    for source_record in consolidated:
        raw = dict(source_record)
        source_system = str(raw.pop("_source_system"))
        bronze_records.append(
            {
                **raw,
                "source_system": source_system,
                "branch_id": raw.get("branch_id"),
                "batch_id": batch_id,
                "flow_run_id": flow_run_id,
                "ingestion_timestamp": ingestion_timestamp,
                "schema_version": raw.get("schema_version"),
                "record_hash": _record_hash(raw),
                "raw_payload": json.dumps(raw, ensure_ascii=False, sort_keys=True),
            }
        )
    artifact = get_storage().write_records(
        "bronze",
        batch_id,
        "transactions",
        bronze_records,
        {"flow_run_id": flow_run_id, "scenario": scenario},
    )
    registrar_telemetria(
        "BRONZE_LOAD",
        batch_id=batch_id,
        flow_run_id=flow_run_id,
        input_records=len(bronze_records),
        task_duration=round(time.perf_counter() - started, 6),
        retry_count=0,
    )
    return artifact

