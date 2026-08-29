"""Canonical Prefect DAG for the FinanData DataOps PoC."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from prefect import flow, unmapped
from prefect.utilities.annotations import opaque

from finandata.config import SCENARIOS
from finandata.observability import registrar_telemetria
from finandata.tasks.ingestion import (
    cargar_bronze,
    extraer_api_bancaria,
    extraer_atm_por_sucursal,
    extraer_core_ach_cdc,
)
from finandata.tasks.publishing import (
    actualizar_bi_analitica,
    generar_reporte_sbs,
    notificar_exito,
    publicar_dataset_riesgo,
)
from finandata.tasks.quality import (
    alertar_y_detener,
    calcular_quality_metrics,
    enviar_schema_quarantine,
    guardar_data_quarantine,
    obtener_registros_validos,
    quality_gate_1,
    validar_data_quality,
    validar_esquema,
)
from finandata.tasks.transformation import (
    persistir_gold,
    persistir_silver,
    transformar_enriquecer,
    transformar_silver,
)
from finandata.tasks.warehouse import (
    bloquear_publicacion,
    merge_upsert_dwh,
    post_load_testing,
    quality_gate_2,
    reconciliar,
)


BRANCH_IDS = ["LIM-001", "LIM-002", "LIM-003"]


def _flow_run_id() -> str:
    try:
        from prefect.runtime import flow_run

        return str(flow_run.id)
    except Exception:
        return str(uuid4())


@flow(name="finandata-dataops-pipeline", log_prints=True)
def finandata_pipeline(scenario: str = "success", batch_id: str | None = None) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise ValueError(f"Escenario no soportado: {scenario}. Use {sorted(SCENARIOS)}")
    started = time.perf_counter()
    actual_batch_id = batch_id or (
        f"{scenario}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:8]}"
    )
    flow_run_id = _flow_run_id()

    # Cinco ejecuciones independientes convergen exclusivamente en cargar_bronze.
    atm_runs = extraer_atm_por_sucursal.map(BRANCH_IDS, scenario=unmapped(scenario))
    api_run = extraer_api_bancaria.submit(scenario)
    cdc_run = extraer_core_ach_cdc.submit(scenario)
    bronze = cargar_bronze.submit(
        atm_runs,
        api_run,
        cdc_run,
        actual_batch_id,
        flow_run_id,
        scenario,
    )

    contract = validar_esquema.submit(opaque(bronze))
    contract_result = contract.result()
    if not contract_result["passed"]:
        schema_quarantine = enviar_schema_quarantine.submit(opaque(contract))
        schema_quarantine_result = schema_quarantine.result()
        alertar_y_detener.submit(opaque(schema_quarantine)).result()
        return {
            "scenario": scenario,
            "batch_id": actual_batch_id,
            "pipeline_status": "BLOCKED_SCHEMA",
            "schema_status": "FAIL",
            "data_quality_executed": False,
            "schema_quarantine": schema_quarantine_result["schema_quarantine"],
            "pipeline_duration": round(time.perf_counter() - started, 6),
        }

    dq = validar_data_quality.submit(opaque(contract))
    valid_records = obtener_registros_validos.submit(opaque(dq))
    data_quarantine = guardar_data_quarantine.submit(opaque(dq))
    metrics = calcular_quality_metrics.submit(opaque(valid_records), opaque(data_quarantine))
    gate1 = quality_gate_1.submit(opaque(metrics))
    gate1_result = gate1.result()
    if not gate1_result["passed"]:
        alertar_y_detener.submit(opaque(gate1)).result()
        summary = {
            "scenario": scenario,
            "batch_id": actual_batch_id,
            "pipeline_status": "FAILED_QUALITY",
            "qg1_status": "FAIL",
            **gate1_result["metrics"],
            "silver_executed": False,
            "gold_executed": False,
            "dwh_executed": False,
            "publications_executed": False,
            "pipeline_duration": round(time.perf_counter() - started, 6),
        }
        registrar_telemetria("PIPELINE_BLOCKED", **summary)
        return summary

    # Los datos válidos y la autorización QG1 son entradas distintas de Silver.
    silver = transformar_silver.submit(opaque(valid_records), opaque(gate1))
    silver_artifact = persistir_silver.submit(opaque(silver))
    enriched = transformar_enriquecer.submit(opaque(silver_artifact))
    gold_artifact = persistir_gold.submit(opaque(enriched))

    # Cadena obligatoria sin edges directos hacia publicaciones.
    loaded = merge_upsert_dwh.submit(opaque(gold_artifact))
    post_load = post_load_testing.submit(opaque(loaded))
    reconciliation = reconciliar.submit(opaque(post_load))
    gate2 = quality_gate_2.submit(opaque(reconciliation))
    gate2_result = gate2.result()
    if not gate2_result["passed"]:
        blocked = bloquear_publicacion.submit(opaque(gate2)).result()
        return {
            "scenario": scenario,
            "batch_id": actual_batch_id,
            "pipeline_status": "BLOCKED_QG2",
            "qg1_status": "PASS",
            "qg2_status": "FAIL",
            "publication_status": blocked["publication_status"],
            "publications_executed": False,
            "pipeline_duration": round(time.perf_counter() - started, 6),
        }

    # Las tres publicaciones dependen sólo de la certificación inmediata QG2.
    sbs = generar_reporte_sbs.submit(opaque(gate2))
    risk = publicar_dataset_riesgo.submit(opaque(gate2))
    bi = actualizar_bi_analitica.submit(opaque(gate2))
    notification = notificar_exito.submit(opaque(sbs), opaque(risk), opaque(bi)).result()
    result = {
        "scenario": scenario,
        "batch_id": actual_batch_id,
        "qg1_status": "PASS",
        "qg2_status": "PASS",
        **notification,
        "pipeline_duration": round(time.perf_counter() - started, 6),
    }
    registrar_telemetria("PIPELINE_COMPLETE", **result)
    return result
