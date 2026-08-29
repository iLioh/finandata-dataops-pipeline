from __future__ import annotations

import sqlite3
from pathlib import Path

from finandata.flows.pipeline import finandata_pipeline
from prefect.settings import (
    PREFECT_LOGGING_TO_API_ENABLED,
    PREFECT_MEMO_STORE_PATH,
    PREFECT_SERVER_ANALYTICS_ENABLED,
    temporary_settings,
)
from prefect.testing.utilities import prefect_test_harness


def configure_local_backends(monkeypatch, tmp_path: Path) -> Path:
    database = tmp_path / "finandata.db"
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("DWH_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.setenv("LOCAL_DWH_PATH", str(database))
    monkeypatch.setenv("ALERTS_FILE", str(tmp_path / "alerts.jsonl"))
    return database


def prefect_quiet_settings() -> dict[object, object]:
    memo_path = Path.cwd() / ".local" / "prefect-test-memo.toml"
    memo_path.parent.mkdir(parents=True, exist_ok=True)
    return {
        PREFECT_LOGGING_TO_API_ENABLED: False,
        PREFECT_SERVER_ANALYTICS_ENABLED: False,
        PREFECT_MEMO_STORE_PATH: memo_path,
    }


def test_incident_flow_stops_before_silver(monkeypatch, tmp_path: Path) -> None:
    database = configure_local_backends(monkeypatch, tmp_path)
    with temporary_settings(updates=prefect_quiet_settings()), prefect_test_harness():
        result = finandata_pipeline(
            scenario="incident_15_percent", batch_id="smoke-incident-15-percent"
        )
    assert result["input_records"] == 100
    assert result["valid_records"] == 85
    assert result["rejected_records"] == 15
    assert result["reject_rate"] == 0.15
    assert result["qg1_status"] == "FAIL"
    assert result["silver_executed"] is False
    assert result["gold_executed"] is False
    assert result["dwh_executed"] is False
    assert result["publications_executed"] is False
    assert not (tmp_path / "lake" / "silver" / result["batch_id"]).exists()
    assert not (tmp_path / "lake" / "gold" / result["batch_id"]).exists()
    with sqlite3.connect(database) as connection:
        fact_count = connection.execute(
            "SELECT COUNT(*) FROM fact_transacciones WHERE batch_id = ?", (result["batch_id"],)
        ).fetchone()[0]
        status = connection.execute(
            "SELECT pipeline_status FROM etl_batch_control WHERE batch_id = ?",
            (result["batch_id"],),
        ).fetchone()[0]
    assert fact_count == 0
    assert status == "FAILED_QUALITY"


def test_success_flow_is_published(monkeypatch, tmp_path: Path) -> None:
    database = configure_local_backends(monkeypatch, tmp_path)
    with temporary_settings(updates=prefect_quiet_settings()), prefect_test_harness():
        result = finandata_pipeline(scenario="success", batch_id="smoke-success")
    assert result["pipeline_status"] == "SUCCESS"
    assert result["qg1_status"] == "PASS"
    assert result["qg2_status"] == "PASS"
    with sqlite3.connect(database) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM fact_transacciones WHERE batch_id = 'smoke-success'"
        ).fetchone()[0]
    assert count == 100


def test_schema_fail_stops_in_schema_quarantine(monkeypatch, tmp_path: Path) -> None:
    database = configure_local_backends(monkeypatch, tmp_path)
    with temporary_settings(updates=prefect_quiet_settings()), prefect_test_harness():
        result = finandata_pipeline(scenario="schema_fail", batch_id="smoke-schema-fail")
    assert result["pipeline_status"] == "BLOCKED_SCHEMA"
    assert result["schema_status"] == "FAIL"
    assert result["data_quality_executed"] is False
    assert result["schema_quarantine"]["record_count"] == 100
    assert (tmp_path / "lake" / result["schema_quarantine"]["path"]).exists()
    with sqlite3.connect(database) as connection:
        fact_count = connection.execute(
            "SELECT COUNT(*) FROM fact_transacciones WHERE batch_id = ?", (result["batch_id"],)
        ).fetchone()[0]
        status = connection.execute(
            "SELECT pipeline_status FROM etl_batch_control WHERE batch_id = ?",
            (result["batch_id"],),
        ).fetchone()[0]
    assert fact_count == 0
    assert status == "BLOCKED_SCHEMA"


def test_qg2_fail_blocks_publications_after_99_of_100(monkeypatch, tmp_path: Path) -> None:
    database = configure_local_backends(monkeypatch, tmp_path)
    with temporary_settings(updates=prefect_quiet_settings()), prefect_test_harness():
        result = finandata_pipeline(scenario="qg2_fail", batch_id="smoke-qg2-fail")
    assert result["pipeline_status"] == "BLOCKED_QG2"
    assert result["qg1_status"] == "PASS"
    assert result["qg2_status"] == "FAIL"
    assert result["publication_status"] == "BLOCKED"
    assert result["publications_executed"] is False
    with sqlite3.connect(database) as connection:
        fact_count = connection.execute(
            "SELECT COUNT(*) FROM fact_transacciones WHERE batch_id = ?", (result["batch_id"],)
        ).fetchone()[0]
        control = connection.execute(
            "SELECT loaded_records, pipeline_status FROM etl_batch_control WHERE batch_id = ?",
            (result["batch_id"],),
        ).fetchone()
    assert fact_count == 99
    assert control == (99, "BLOCKED_QG2")
