"""Run one local demonstration scenario."""

from __future__ import annotations

import argparse
import inspect
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ["PREFECT_HOME"] = str(ROOT / ".local" / "prefect")
os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"
os.environ["PREFECT_LOGGING_TO_API_ENABLED"] = "false"
os.environ["PREFECT_MEMO_STORE_PATH"] = str(ROOT / ".local" / "prefect" / "memo_store.toml")

from finandata.config import SCENARIOS  # noqa: E402
from finandata.flows.pipeline import finandata_pipeline  # noqa: E402
from prefect.settings import (  # noqa: E402
    PREFECT_API_URL,
    PREFECT_HOME,
    PREFECT_LOGGING_TO_API_ENABLED,
    PREFECT_MEMO_STORE_PATH,
    PREFECT_SERVER_ANALYTICS_ENABLED,
    PREFECT_SERVER_DATABASE_CONNECTION_URL,
    PREFECT_SERVER_EPHEMERAL_ENABLED,
    temporary_settings,
)
from prefect.events.worker import EventsWorker  # noqa: E402
from prefect.server.api.server import SubprocessASGIServer  # noqa: E402
from prefect.utilities.asyncutils import run_coro_as_sync  # noqa: E402


def run_with_local_prefect(scenario: str, batch_id: str | None) -> dict[str, object]:
    """Run against a persisted local Prefect DB and stop the server cleanly."""
    prefect_home = ROOT / ".local" / "prefect"
    prefect_home.mkdir(parents=True, exist_ok=True)
    prefect_db_url = f"sqlite+aiosqlite:///{(prefect_home / 'prefect.db').as_posix()}"
    settings = {
        PREFECT_API_URL: None,
        PREFECT_HOME: prefect_home,
        PREFECT_LOGGING_TO_API_ENABLED: False,
        PREFECT_MEMO_STORE_PATH: prefect_home / "memo_store.toml",
        PREFECT_SERVER_ANALYTICS_ENABLED: False,
        PREFECT_SERVER_DATABASE_CONNECTION_URL: prefect_db_url,
        PREFECT_SERVER_EPHEMERAL_ENABLED: True,
    }
    with temporary_settings(updates=settings):
        server = SubprocessASGIServer()
        server.start()
        try:
            with temporary_settings(updates={PREFECT_API_URL: server.api_url}):
                result = finandata_pipeline(scenario=scenario, batch_id=batch_id)
        finally:
            async def drain_events() -> None:
                drained = EventsWorker.drain_all()
                if inspect.isawaitable(drained):
                    await drained

            run_coro_as_sync(drain_events())
            server.stop()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta un escenario DataOps de FinanData")
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--batch-id", help="Identificador para reejecución idempotente")
    args = parser.parse_args()
 
    # La demo es autosuficiente: ignora perfiles externos y usa orquestación
    # efímera local, nunca Prefect Cloud.
    result = run_with_local_prefect(args.scenario, args.batch_id)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
