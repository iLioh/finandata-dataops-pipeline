"""Structured telemetry emitted to Prefect logs or standard logging."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any


_SENSITIVE_PARTS = ("service_role_key", "password", "secret", "db_url")


def _safe_value(key: str, value: Any) -> Any:
    lower = key.lower()
    if any(part in lower for part in _SENSITIVE_PARTS):
        return "***REDACTED***"
    if "iban" in lower and value:
        clean = str(value).replace(" ", "")
        return f"****{clean[-4:]}"
    if isinstance(value, (datetime, Decimal)):
        return str(value)
    return value


def _logger() -> Any:
    try:
        from prefect import get_run_logger

        return get_run_logger()
    except Exception:
        return logging.getLogger("finandata.telemetry")


def registrar_telemetria(evento: str, **metricas: Any) -> dict[str, Any]:
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": evento,
        **{key: _safe_value(key, value) for key, value in metricas.items()},
    }
    _logger().info("TELEMETRY %s", json.dumps(payload, ensure_ascii=False, default=str))
    return payload

