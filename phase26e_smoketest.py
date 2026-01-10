#!/usr/bin/env python3
"""
phase26e_smoketest.py

Applies Phase 26E SQL and runs the dryrun order.place outbox runner once.
"""

import os
import subprocess
from pathlib import Path

def main():
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DB_URL/DATABASE_URL missing")

    root = Path(__file__).resolve().parent
    sql = root / "sql" / "alpha_dryrun_orderplace.sql"
    if not sql.exists():
        raise RuntimeError(f"Missing {sql}")

    subprocess.check_call(["psql", db_url, "-v", "ON_ERROR_STOP=1", "-f", str(sql)])

    from alpha_outbox_orderplace_dryrun import run
    run(limit=50)

if __name__ == "__main__":
    main()
