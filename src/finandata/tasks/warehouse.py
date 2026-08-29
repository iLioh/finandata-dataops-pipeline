"""Idempotent DWH loading, post-load controls, reconciliation and QG2."""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from prefect import task

from finandata.alerts import emitir_alerta
from finandata.config import PROJECT_ROOT, Settings
from finandata.observability import registrar_telemetria
from finandata.storage import get_storage


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_sucursal (
    branch_id TEXT PRIMARY KEY, branch_name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_canal (
    channel TEXT PRIMARY KEY, channel_name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_moneda (
    currency TEXT PRIMARY KEY, currency_name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_transacciones (
    transaction_id TEXT PRIMARY KEY,
    transaction_date TEXT NOT NULL,
    branch_id TEXT NOT NULL REFERENCES dim_sucursal(branch_id),
    channel TEXT NOT NULL REFERENCES dim_canal(channel),
    currency TEXT NOT NULL REFERENCES dim_moneda(currency),
    amount NUMERIC NOT NULL,
    commission NUMERIC NOT NULL,
    debit_amount NUMERIC NOT NULL,
    credit_amount NUMERIC NOT NULL,
    risk_score INTEGER NOT NULL,
    iban_masked TEXT NOT NULL,
    source_system TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    flow_run_id TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    loaded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_batch ON fact_transacciones(batch_id);
CREATE TABLE IF NOT EXISTS etl_batch_control (
    batch_id TEXT PRIMARY KEY,
    flow_run_id TEXT NOT NULL,
    fecha_proceso TEXT NOT NULL,
    input_records INTEGER NOT NULL DEFAULT 0,
    valid_records INTEGER NOT NULL DEFAULT 0,
    rejected_records INTEGER NOT NULL DEFAULT 0,
    reject_rate NUMERIC NOT NULL DEFAULT 0,
    qg1_status TEXT NOT NULL DEFAULT 'NOT_EVALUATED',
    loaded_records INTEGER NOT NULL DEFAULT 0,
    qg2_status TEXT NOT NULL DEFAULT 'NOT_EVALUATED',
    pipeline_status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
"""


BATCH_FIELDS = (
    "batch_id",
    "flow_run_id",
    "fecha_proceso",
    "input_records",
    "valid_records",
    "rejected_records",
    "reject_rate",
    "qg1_status",
    "loaded_records",
    "qg2_status",
    "pipeline_status",
    "started_at",
    "finished_at",
)


class Warehouse:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.from_env()
        self.settings.validate_dwh()

    @contextmanager
    def connection(self) -> Iterator[Any]:
        if self.settings.dwh_backend == "local":
            self.settings.local_dwh_path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.settings.local_dwh_path)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
        else:
            import psycopg

            connection = psycopg.connect(self.settings.supabase_db_url)
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connection() as connection:
            if self.settings.dwh_backend == "local":
                connection.executescript(SQLITE_SCHEMA)
                connection.executemany(
                    "INSERT INTO dim_sucursal VALUES (?, ?) "
                    "ON CONFLICT(branch_id) DO UPDATE SET branch_name=excluded.branch_name",
                    [("LIM-001", "Lima Centro"), ("LIM-002", "Lima Norte"), ("LIM-003", "Lima Sur")],
                )
                connection.executemany(
                    "INSERT INTO dim_canal VALUES (?, ?) "
                    "ON CONFLICT(channel) DO UPDATE SET channel_name=excluded.channel_name",
                    [("ATM", "Cajero automático"), ("MOBILE", "Banca móvil"), ("ACH", "Core / ACH")],
                )
                connection.executemany(
                    "INSERT INTO dim_moneda VALUES (?, ?) "
                    "ON CONFLICT(currency) DO UPDATE SET currency_name=excluded.currency_name",
                    [("PEN", "Sol peruano"), ("USD", "Dólar estadounidense")],
                )
            else:
                schema = (PROJECT_ROOT / "sql" / "dwh_schema.sql").read_text(encoding="utf-8")
                connection.execute(schema)

    def upsert_batch_control(self, values: dict[str, Any]) -> None:
        self.initialize()
        now = datetime.now(UTC).isoformat()
        defaults: dict[str, Any] = {
            "batch_id": values["batch_id"],
            "flow_run_id": values.get("flow_run_id", "unknown"),
            "fecha_proceso": now[:10],
            "input_records": 0,
            "valid_records": 0,
            "rejected_records": 0,
            "reject_rate": 0,
            "qg1_status": "NOT_EVALUATED",
            "loaded_records": 0,
            "qg2_status": "NOT_EVALUATED",
            "pipeline_status": "RUNNING",
            "started_at": now,
            "finished_at": None,
        }
        with self.connection() as connection:
            marker = "?" if self.settings.dwh_backend == "local" else "%s"
            existing = connection.execute(
                f"SELECT * FROM etl_batch_control WHERE batch_id = {marker}",
                (values["batch_id"],),
            ).fetchone()
            if existing:
                if self.settings.dwh_backend == "local":
                    defaults.update(dict(existing))
                else:
                    defaults.update(dict(zip(BATCH_FIELDS, existing, strict=True)))
            defaults.update(values)
            placeholders = ", ".join([marker] * len(BATCH_FIELDS))
            updates = ", ".join(
                f"{field}=excluded.{field}" for field in BATCH_FIELDS if field != "batch_id"
            )
            connection.execute(
                f"INSERT INTO etl_batch_control ({', '.join(BATCH_FIELDS)}) "
                f"VALUES ({placeholders}) ON CONFLICT(batch_id) DO UPDATE SET {updates}",
                tuple(defaults[field] for field in BATCH_FIELDS),
            )

    def upsert_transactions(self, records: list[dict[str, Any]]) -> None:
        self.initialize()
        fields = (
            "transaction_id",
            "transaction_date",
            "branch_id",
            "channel",
            "currency",
            "amount",
            "commission",
            "debit_amount",
            "credit_amount",
            "risk_score",
            "iban_masked",
            "source_system",
            "batch_id",
            "flow_run_id",
            "record_hash",
            "loaded_at",
        )
        marker = "?" if self.settings.dwh_backend == "local" else "%s"
        placeholders = ", ".join([marker] * len(fields))
        updates = ", ".join(
            f"{field}=excluded.{field}" for field in fields if field != "transaction_id"
        )
        sql = (
            f"INSERT INTO fact_transacciones ({', '.join(fields)}) VALUES ({placeholders}) "
            f"ON CONFLICT(transaction_id) DO UPDATE SET {updates}"
        )
        now = datetime.now(UTC).isoformat()
        values = []
        for record in records:
            row = {**record, "loaded_at": now}
            if self.settings.dwh_backend == "local":
                for money_field in ("amount", "commission", "debit_amount", "credit_amount"):
                    row[money_field] = str(row[money_field])
            values.append(tuple(row[field] for field in fields))
        with self.connection() as connection:
            cursor = connection.cursor()
            try:
                cursor.executemany(sql, values)
            finally:
                cursor.close()

    def batch_snapshot(self, batch_id: str) -> dict[str, Any]:
        self.initialize()
        marker = "?" if self.settings.dwh_backend == "local" else "%s"
        with self.connection() as connection:
            rows = connection.execute(
                f"SELECT transaction_id, amount, debit_amount, credit_amount "
                f"FROM fact_transacciones WHERE batch_id = {marker}",
                (batch_id,),
            ).fetchall()
        amounts = [Decimal(str(row[1])) for row in rows]
        debits = [Decimal(str(row[2])) for row in rows]
        credits = [Decimal(str(row[3])) for row in rows]
        ids = [str(row[0]) for row in rows]
        return {
            "loaded_records": len(rows),
            "amount_total": sum(amounts, Decimal("0")),
            "debit_total": sum(debits, Decimal("0")),
            "credit_total": sum(credits, Decimal("0")),
            "duplicate_count": len(ids) - len(set(ids)),
        }


def get_warehouse() -> Warehouse:
    return Warehouse()


def record_batch_control(**values: Any) -> None:
    get_warehouse().upsert_batch_control(values)


@task(name="merge_upsert_dwh", retries=3, retry_delay_seconds=[1, 2, 4])
def merge_upsert_dwh(gold_artifact: dict[str, Any]) -> dict[str, Any]:
    started = time.perf_counter()
    metadata = gold_artifact["metadata"]
    records = get_storage().read_records(gold_artifact)
    rows_to_load = records[:-1] if metadata["scenario"] == "qg2_fail" else records
    try:
        warehouse = get_warehouse()
        warehouse.upsert_transactions(rows_to_load)
        metrics = metadata["metrics"]
        record_batch_control(
            batch_id=gold_artifact["batch_id"],
            flow_run_id=metadata["flow_run_id"],
            input_records=metrics["input_records"],
            valid_records=metrics["valid_records"],
            rejected_records=metrics["rejected_records"],
            reject_rate=metrics["reject_rate"],
            qg1_status="PASS",
            loaded_records=len(rows_to_load),
            qg2_status="NOT_EVALUATED",
            pipeline_status="LOADED_PENDING_CERTIFICATION",
        )
    except Exception as exc:
        emitir_alerta(
            "DWH_LOAD_FAILED",
            f"Falló la carga idempotente al DWH: {type(exc).__name__}",
            batch_id=gold_artifact["batch_id"],
            flow_run_id=metadata["flow_run_id"],
            task_name="merge_upsert_dwh",
        )
        raise
    registrar_telemetria(
        "DWH_LOAD",
        batch_id=gold_artifact["batch_id"],
        dwh_expected_records=metadata["expected_count"],
        dwh_loaded_records=len(rows_to_load),
        task_duration=round(time.perf_counter() - started, 6),
        retry_count=0,
    )
    return {
        "batch_id": gold_artifact["batch_id"],
        "flow_run_id": metadata["flow_run_id"],
        "scenario": metadata["scenario"],
        "expected_count": metadata["expected_count"],
        "expected_amount": metadata["expected_amount"],
        "attempted_records": len(rows_to_load),
    }


def evaluar_post_load(expected_count: int, snapshot: dict[str, Any]) -> dict[str, bool]:
    return {
        "debit_equals_credit": snapshot["debit_total"] == snapshot["credit_total"],
        "expected_equals_loaded": expected_count == snapshot["loaded_records"],
        "duplicates_zero": snapshot["duplicate_count"] == 0,
        "batch_complete": expected_count == snapshot["loaded_records"],
    }


@task(name="post_load_testing", retries=0)
def post_load_testing(load_result: dict[str, Any]) -> dict[str, Any]:
    snapshot = get_warehouse().batch_snapshot(load_result["batch_id"])
    checks = evaluar_post_load(int(load_result["expected_count"]), snapshot)
    passed = all(checks.values())
    result = {
        "passed": passed,
        "checks": checks,
        "batch_id": load_result["batch_id"],
        "flow_run_id": load_result["flow_run_id"],
        "scenario": load_result["scenario"],
        "expected_count": load_result["expected_count"],
        "expected_amount": load_result["expected_amount"],
        "loaded_records": snapshot["loaded_records"],
    }
    registrar_telemetria(
        "POST_LOAD",
        batch_id=load_result["batch_id"],
        dwh_expected_records=load_result["expected_count"],
        dwh_loaded_records=snapshot["loaded_records"],
        post_load_status="PASS" if passed else "FAIL",
    )
    if not passed:
        emitir_alerta(
            "POST_LOAD_FAILED",
            "Las pruebas post-load detectaron una carga incompleta o inconsistente.",
            batch_id=load_result["batch_id"],
            flow_run_id=load_result["flow_run_id"],
            task_name="post_load_testing",
            metric_value=checks,
            threshold={"all_checks": True},
        )
    return result


def evaluar_reconciliacion(
    expected_count: int,
    expected_amount: Decimal,
    loaded_count: int,
    loaded_amount: Decimal,
) -> dict[str, Any]:
    count_diff = loaded_count - expected_count
    amount_diff = loaded_amount - expected_amount
    return {
        "passed": count_diff == 0 and amount_diff == Decimal("0"),
        "expected_count": expected_count,
        "loaded_count": loaded_count,
        "expected_amount": expected_amount,
        "loaded_amount": loaded_amount,
        "count_diff": count_diff,
        "amount_diff": amount_diff,
        "control_totals_match": count_diff == 0 and amount_diff == Decimal("0"),
    }


@task(name="reconciliar", retries=0)
def reconciliar(post_load_result: dict[str, Any]) -> dict[str, Any]:
    snapshot = get_warehouse().batch_snapshot(post_load_result["batch_id"])
    evidence = evaluar_reconciliacion(
        int(post_load_result["expected_count"]),
        Decimal(str(post_load_result["expected_amount"])),
        int(snapshot["loaded_records"]),
        Decimal(str(snapshot["amount_total"])),
    )
    result = {
        **evidence,
        "post_load_passed": post_load_result["passed"],
        "batch_id": post_load_result["batch_id"],
        "flow_run_id": post_load_result["flow_run_id"],
        "scenario": post_load_result["scenario"],
    }
    registrar_telemetria(
        "RECONCILIATION",
        batch_id=post_load_result["batch_id"],
        reconciliation_count_diff=evidence["count_diff"],
        reconciliation_amount_diff=evidence["amount_diff"],
    )
    if not evidence["passed"]:
        emitir_alerta(
            "RECONCILIATION_FAILED",
            "La reconciliación detectó diferencias de conteo o monto.",
            batch_id=post_load_result["batch_id"],
            flow_run_id=post_load_result["flow_run_id"],
            task_name="reconciliar",
            metric_value={
                "count_diff": evidence["count_diff"],
                "amount_diff": str(evidence["amount_diff"]),
            },
            threshold=0,
        )
    return result


def evaluar_quality_gate_2(reconciliation: dict[str, Any]) -> bool:
    return bool(reconciliation["passed"] and reconciliation["post_load_passed"])


@task(name="quality_gate_2", retries=0)
def quality_gate_2(reconciliation: dict[str, Any]) -> dict[str, Any]:
    passed = evaluar_quality_gate_2(reconciliation)
    status = "PASS" if passed else "FAIL"
    result = {
        "passed": passed,
        "status": status,
        "batch_id": reconciliation["batch_id"],
        "flow_run_id": reconciliation["flow_run_id"],
        "scenario": reconciliation["scenario"],
    }
    registrar_telemetria(
        "QUALITY_GATE_2",
        batch_id=result["batch_id"],
        qg2_status=status,
        reconciliation_count_diff=reconciliation["count_diff"],
        reconciliation_amount_diff=reconciliation["amount_diff"],
    )
    if not passed:
        emitir_alerta(
            "QUALITY_GATE_2_FAILED",
            "QG2 bloqueó todas las publicaciones.",
            batch_id=result["batch_id"],
            flow_run_id=result["flow_run_id"],
            task_name="quality_gate_2",
            metric_value={
                "post_load_passed": reconciliation["post_load_passed"],
                "reconciliation_passed": reconciliation["passed"],
            },
            threshold={"both": True},
        )
        record_batch_control(
            batch_id=result["batch_id"],
            flow_run_id=result["flow_run_id"],
            qg1_status="PASS",
            qg2_status="FAIL",
            pipeline_status="BLOCKED_QG2",
            finished_at=datetime.now(UTC).isoformat(),
        )
    else:
        record_batch_control(
            batch_id=result["batch_id"],
            flow_run_id=result["flow_run_id"],
            qg1_status="PASS",
            qg2_status="PASS",
            pipeline_status="CERTIFIED_PENDING_PUBLICATION",
        )
    return result


@task(name="bloquear_publicacion", retries=0)
def bloquear_publicacion(gate2: dict[str, Any]) -> dict[str, Any]:
    if gate2["passed"]:
        raise ValueError("No se puede bloquear un lote certificado")
    return {
        "batch_id": gate2["batch_id"],
        "publication_status": "BLOCKED",
        "reason": "QUALITY_GATE_2_FAILED",
    }
