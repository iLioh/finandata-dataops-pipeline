"""Storage abstraction for local tests and Supabase Storage."""

from __future__ import annotations

import io
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

import pandas as pd

from finandata.config import Settings


DATA_LAKE_ZONES = frozenset(
    {"bronze", "silver", "gold", "schema-quarantine", "data-quarantine"}
)


class StorageClient:
    """Persist Parquet artifacts behind a local or Supabase backend."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.from_env()
        self.settings.validate_storage()
        self._supabase: Any | None = None

    def _object_path(self, zone: str, batch_id: str, name: str) -> str:
        if zone not in DATA_LAKE_ZONES:
            raise ValueError(f"Zona de Data Lake no permitida: {zone}")
        safe_name = name if name.endswith(".parquet") else f"{name}.parquet"
        return str(PurePosixPath(zone) / batch_id / safe_name)

    def _get_supabase(self) -> Any:
        if self._supabase is None:
            from supabase import create_client

            self._supabase = create_client(
                self.settings.supabase_url or "", self.settings.supabase_service_role_key or ""
            )
        return self._supabase

    def write_records(
        self,
        zone: str,
        batch_id: str,
        name: str,
        records: Iterable[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rows = list(records)
        object_path = self._object_path(zone, batch_id, name)
        frame = pd.DataFrame(rows)
        if self.settings.storage_backend == "local":
            local_path = self.settings.local_data_lake_root.joinpath(*PurePosixPath(object_path).parts)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(local_path, index=False)
        else:
            buffer = io.BytesIO()
            frame.to_parquet(buffer, index=False)
            bucket = self._get_supabase().storage.from_(self.settings.storage_bucket)
            bucket.upload(
                path=object_path,
                file=buffer.getvalue(),
                file_options={"content-type": "application/octet-stream", "upsert": "true"},
            )
        return {
            "zone": zone,
            "path": object_path,
            "batch_id": batch_id,
            "record_count": len(rows),
            "metadata": metadata or {},
        }

    def read_records(self, artifact: dict[str, Any]) -> list[dict[str, Any]]:
        object_path = str(artifact["path"])
        if self.settings.storage_backend == "local":
            local_path = self.settings.local_data_lake_root.joinpath(*PurePosixPath(object_path).parts)
            if not local_path.exists():
                raise FileNotFoundError(local_path)
            frame = pd.read_parquet(local_path)
        else:
            payload = (
                self._get_supabase()
                .storage.from_(self.settings.storage_bucket)
                .download(object_path)
            )
            frame = pd.read_parquet(io.BytesIO(payload))
        return frame.where(pd.notna(frame), None).to_dict(orient="records")

    def local_path(self, artifact: dict[str, Any]) -> Path | None:
        if self.settings.storage_backend != "local":
            return None
        return self.settings.local_data_lake_root.joinpath(
            *PurePosixPath(str(artifact["path"])).parts
        )


def get_storage() -> StorageClient:
    return StorageClient()

