#!/usr/bin/env python3
# phase26d_smoketest.py — applies DB DDL for previews + runs outbox preview once

import os
import subprocess
from pathlib import Path

from alpha_outbox_preview import run_alpha_outbox_preview

ROOT = Path(__file__).resolve().parent
SQL = ROOT / "sql" / "alpha_command_previews.sql"

def main():
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DB_URL/DATABASE_URL missing")
    if not SQL.exists():
        raise RuntimeError(f"Missing {SQL}")

    subprocess.check_call(["psql", db_url, "-v", "ON_ERROR_STOP=1", "-f", str(SQL)])
    run_alpha_outbox_preview(limit=50)

if __name__ == "__main__":
    main()
