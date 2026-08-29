"""Environment and policy configuration for the PoC."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRANCHES = frozenset({"LIM-001", "LIM-002", "LIM-003"})
CURRENCIES = frozenset({"PEN", "USD"})
CHANNELS = frozenset({"ATM", "MOBILE", "ACH"})
SCENARIOS = frozenset({"success", "schema_fail", "incident_15_percent", "qg2_fail"})


def _absolute_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    storage_backend: str
    storage_bucket: str
    local_data_lake_root: Path
    dwh_backend: str
    local_dwh_path: Path
    supabase_url: str | None
    supabase_service_role_key: str | None
    supabase_db_url: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv(PROJECT_ROOT / ".env", override=False)
        storage_backend = os.getenv("STORAGE_BACKEND", "local").lower()
        dwh_backend = os.getenv("DWH_BACKEND", "local").lower()
        if storage_backend not in {"local", "supabase"}:
            raise ValueError("STORAGE_BACKEND debe ser 'local' o 'supabase'")
        if dwh_backend not in {"local", "postgres"}:
            raise ValueError("DWH_BACKEND debe ser 'local' o 'postgres'")
        return cls(
            storage_backend=storage_backend,
            storage_bucket=os.getenv("SUPABASE_STORAGE_BUCKET", "finandata-data-lake"),
            local_data_lake_root=_absolute_path(
                os.getenv("LOCAL_DATA_LAKE_ROOT", ".local/data-lake")
            ),
            dwh_backend=dwh_backend,
            local_dwh_path=_absolute_path(os.getenv("LOCAL_DWH_PATH", ".local/finandata.db")),
            supabase_url=os.getenv("SUPABASE_URL") or None,
            supabase_service_role_key=os.getenv("SUPABASE_SERVICE_ROLE_KEY") or None,
            supabase_db_url=os.getenv("SUPABASE_DB_URL") or None,
        )

    def validate_storage(self) -> None:
        if self.storage_backend == "supabase" and not (
            self.supabase_url and self.supabase_service_role_key
        ):
            raise ValueError(
                "SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY son obligatorios para Supabase Storage"
            )

    def validate_dwh(self) -> None:
        if self.dwh_backend == "postgres" and not self.supabase_db_url:
            raise ValueError("SUPABASE_DB_URL es obligatorio para el DWH PostgreSQL")


def load_quality_policy(path: Path | None = None) -> dict[str, float]:
    policy_path = path or PROJECT_ROOT / "config" / "quality_policy.json"
    with policy_path.open(encoding="utf-8") as policy_file:
        policy: dict[str, Any] = json.load(policy_file)
    required = {"max_reject_rate", "min_completeness", "max_duplicate_rate"}
    missing = required.difference(policy)
    if missing:
        raise ValueError(f"Política de calidad incompleta: {sorted(missing)}")
    return {key: float(policy[key]) for key in required}

