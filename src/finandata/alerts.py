"""Structured local alerts for auditable PoC evidence."""

from __future__ import annotations

import json
import os
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from finandata.config import PROJECT_ROOT
from finandata.observability import _logger


_ALERT_LOCK = threading.Lock()


def emitir_alerta(
    event_type: str,
    message: str,
    *,
    severity: str = "ERROR",
    batch_id: str | None = None,
    flow_run_id: str | None = None,
    task_name: str | None = None,
    source_system: str | None = None,
    metric_value: Any = None,
    threshold: Any = None,
) -> dict[str, Any]:
    payload = {
        "severity": severity,
        "event_type": event_type,
        "timestamp": datetime.now(UTC).isoformat(),
        "batch_id": batch_id,
        "flow_run_id": flow_run_id,
        "task_name": task_name,
        "source_system": source_system,
        "message": message,
        "metric_value": metric_value,
        "threshold": threshold,
    }
    _logger().error("ALERT %s", json.dumps(payload, ensure_ascii=False, default=str))
    alert_path = Path(os.getenv("ALERTS_FILE", str(PROJECT_ROOT / "logs" / "alerts.jsonl")))
    alert_path.parent.mkdir(parents=True, exist_ok=True)
    with _ALERT_LOCK, alert_path.open("a", encoding="utf-8") as alert_file:
        alert_file.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    return payload

