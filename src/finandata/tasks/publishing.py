"""Parallel downstream publication tasks gated only by QG2."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import task

from finandata.alerts import emitir_alerta
from finandata.config import PROJECT_ROOT
from finandata.observability import registrar_telemetria
from finandata.tasks.warehouse import record_batch_control


def _publish(gate2: dict[str, Any], target: str, event: str) -> dict[str, Any]:
    if not gate2["passed"]:
        raise RuntimeError(f"{target} no está autorizado por QG2")
    output_dir = PROJECT_ROOT / ".local" / "publications" / gate2["batch_id"]
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence = {
        "target": target,
        "batch_id": gate2["batch_id"],
        "flow_run_id": gate2["flow_run_id"],
        "publication_status": "PUBLISHED",
        "published_at": datetime.now(UTC).isoformat(),
    }
    (output_dir / f"{target.lower()}.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    registrar_telemetria(event, **evidence)
    return evidence


@task(name="generar_reporte_sbs", retries=2, retry_delay_seconds=[1, 2])
def generar_reporte_sbs(gate2: dict[str, Any]) -> dict[str, Any]:
    try:
        return _publish(gate2, "SBS", "PUBLISH_SBS")
    except Exception as exc:
        emitir_alerta(
            "PUBLISH_SBS_FAILED",
            f"Falló la generación del reporte SBS: {type(exc).__name__}",
            batch_id=gate2.get("batch_id"),
            flow_run_id=gate2.get("flow_run_id"),
            task_name="generar_reporte_sbs",
        )
        raise


@task(name="publicar_dataset_riesgo", retries=2, retry_delay_seconds=[1, 2])
def publicar_dataset_riesgo(gate2: dict[str, Any]) -> dict[str, Any]:
    return _publish(gate2, "RISK", "PUBLISH_RISK")


@task(name="actualizar_bi_analitica", retries=2, retry_delay_seconds=[1, 2])
def actualizar_bi_analitica(gate2: dict[str, Any]) -> dict[str, Any]:
    return _publish(gate2, "BI", "PUBLISH_BI")


@task(name="notificar_exito", retries=0)
def notificar_exito(
    sbs: dict[str, Any], risk: dict[str, Any], bi: dict[str, Any]
) -> dict[str, Any]:
    publications = (sbs, risk, bi)
    if any(item["publication_status"] != "PUBLISHED" for item in publications):
        raise RuntimeError("No se puede notificar éxito con publicaciones incompletas")
    batch_id = sbs["batch_id"]
    finished_at = datetime.now(UTC).isoformat()
    record_batch_control(
        batch_id=batch_id,
        flow_run_id=sbs["flow_run_id"],
        qg1_status="PASS",
        qg2_status="PASS",
        pipeline_status="SUCCESS",
        finished_at=finished_at,
    )
    registrar_telemetria(
        "PIPELINE_SUCCESS",
        batch_id=batch_id,
        publication_status="PUBLISHED",
        publications=[item["target"] for item in publications],
    )
    return {
        "batch_id": batch_id,
        "pipeline_status": "SUCCESS",
        "publication_status": "PUBLISHED",
        "finished_at": finished_at,
    }
