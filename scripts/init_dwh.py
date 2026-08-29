"""Initialize the configured local or Supabase PostgreSQL DWH."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from finandata.tasks.warehouse import get_warehouse  # noqa: E402


if __name__ == "__main__":
    get_warehouse().initialize()
    print("Data Warehouse inicializado.")

