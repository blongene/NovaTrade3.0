#!/usr/bin/env python3
# phase26c_smoketest.py — creates DB objects + runs translation preview once

import os
import subprocess
from pathlib import Path

from alpha_translation_preview import run_alpha_translation_preview

ROOT = Path(__file__).resolve().parent
SQL = ROOT / "sql" / "alpha_translations.sql"

def main():
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DB_URL/DATABASE_URL missing")
    if not SQL.exists():
        raise RuntimeError(f"Missing {SQL}")

    # Apply DDL
    subprocess.check_call(["psql", db_url, "-v", "ON_ERROR_STOP=1", "-f", str(SQL)])

    # Run once
    run_alpha_translation_preview()

if __name__ == "__main__":
    main()
