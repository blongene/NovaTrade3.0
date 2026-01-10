#!/usr/bin/env python3
"""Phase 26B smoketest.

1) Applies DB DDL for alpha_approvals (safe idempotent).
2) Runs the Alpha Approvals Sync once.

This does NOT trade. It only writes governance metadata.
"""

import os
import subprocess
from pathlib import Path

from alpha_approvals_sync import run_alpha_approvals_sync

ROOT = Path(__file__).resolve().parent
SQL = ROOT / "sql" / "alpha_approvals.sql"


def _db_url() -> str:
    return os.getenv("DB_URL") or os.getenv("DATABASE_URL") or ""


def _run_psql(sql_path: Path) -> None:
    db = _db_url()
    if not db:
        raise RuntimeError("DB_URL/DATABASE_URL not set")
    psql = os.getenv("PSQL_BIN", "psql")
    cmd = [psql, db, "-v", "ON_ERROR_STOP=1", "-f", str(sql_path)]
    subprocess.check_call(cmd, cwd=str(ROOT))


def main() -> None:
    # Ensure preview flags are present so the sync behaves consistently
    os.environ.setdefault("PREVIEW_ENABLED", "1")
    os.environ.setdefault("ALPHA_PREVIEW_PROPOSALS_ENABLED", "1")

    if not SQL.exists():
        raise FileNotFoundError(f"Missing {SQL}")

    _run_psql(SQL)
    run_alpha_approvals_sync()


if __name__ == "__main__":
    main()
