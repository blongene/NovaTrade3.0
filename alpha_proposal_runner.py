#!/usr/bin/env python3
"""
alpha_proposal_runner.py — Phase 26A (v1.3.1 SQL-native, bulletproof)

Why this exists:
- Your Phase 26 logic already lives in canonical SQL files.
- This runner should ONLY orchestrate those SQL files and never re-implement gates in Python.
- It must be compatible with phase26a_smoketest.py, which imports:
      from alpha_proposal_runner import run_alpha_proposal_runner

Safety:
- Preview-only. Requires:
    PREVIEW_ENABLED=1
    ALPHA_PREVIEW_PROPOSALS_ENABLED=1
- Never enqueues commands / never executes trades.

Execution model:
1) Run sql/alpha_tools.sql in SAFE mode (force preview_enabled=0) to ensure helper objects exist.
2) Run sql/alpha_proposal_generator.sql with preview_enabled=1 to generate preview proposals.
3) Optionally run sql/alpha_polish.sql with preview_enabled=1.

This file is intentionally self-contained and avoids shim hacks that can break imports.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

ROOT = Path(__file__).resolve().parent
SQL_DIR = ROOT / "sql"

# SQL filenames (overrideable)
TOOLS_SQL = os.getenv("ALPHA_TOOLS_SQL", "alpha_tools.sql")
GEN_SQL = os.getenv("ALPHA_GENERATOR_SQL", "alpha_proposal_generator.sql")
POLISH_SQL = os.getenv("ALPHA_POLISH_SQL", "alpha_polish.sql")

POLISH_ENABLED = os.getenv("ALPHA_POLISH_ENABLED", "1").strip().lower() not in ("0", "false", "no")

# Safety gates
PREVIEW_ENABLED = os.getenv("PREVIEW_ENABLED", "0").strip().lower() in ("1", "true", "yes")
ALPHA_ENABLED = os.getenv("ALPHA_PREVIEW_PROPOSALS_ENABLED", "0").strip().lower() in ("1", "true", "yes")

# psql binary name/path
PSQL_BIN = os.getenv("PSQL_BIN", "psql")

# Remove any existing \set preview_enabled ... lines from tools and force to 0
_PREVIEW_SET_RE = re.compile(r"^\s*\\set\s+preview_enabled\s+.*$", re.IGNORECASE | re.MULTILINE)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _log(level: str, msg: str) -> None:
    print(f"[{_utc_now()}] {level.upper():5s} {msg}", flush=True)


@dataclass
class RunResult:
    ok: bool
    returncode: int
    stdout: str
    stderr: str


def _run_cmd(cmd: list[str], env: Optional[dict] = None, cwd: Optional[Path] = None) -> RunResult:
    proc = subprocess.run(
        cmd,
        env=env,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
    )
    return RunResult(
        ok=(proc.returncode == 0),
        returncode=proc.returncode,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
    )


def _require_db_url() -> str:
    db_url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DB_URL/DATABASE_URL is not set (required for Phase 26A runner).")
    return db_url


def _sql_path(name: str) -> Path:
    p = SQL_DIR / name
    if not p.exists():
        raise FileNotFoundError(f"Missing SQL file: {p}")
    return p


def _patched_tools_sql(original: str) -> str:
    """
    Force preview_enabled=0 for alpha_tools.sql so it cannot accidentally generate preview rows.
    """
    stripped = re.sub(_PREVIEW_SET_RE, "", original)
    return "\\set preview_enabled 0\n" + stripped


def _write_temp_sql(contents: str) -> Path:
    fd, path = tempfile.mkstemp(prefix="alpha_tools_safe_", suffix=".sql")
    os.close(fd)
    p = Path(path)
    p.write_text(contents, encoding="utf-8")
    return p


def _parse_generated_count(text: str) -> int:
    """
    Best-effort parse from SQL output. Safe to return 0 if not detectable.
    """
    if not text:
        return 0
    for pat in (
        re.compile(r"generated\D+(\d+)", re.IGNORECASE),
        re.compile(r"wrote\D+(\d+)", re.IGNORECASE),
        re.compile(r"inserted\D+(\d+)", re.IGNORECASE),
    ):
        m = pat.search(text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return 0


def run_alpha_proposal_runner() -> Tuple[int, str]:
    """
    Public entrypoint (required by phase26a_smoketest.py).

    Returns:
      (generated_new, status_string)

    Logging semantics:
      - generated_new: how many *new* proposals were inserted this run (delta).
      - proposals_today_total: how many proposals exist for the current UTC day (snapshot).
        This should match what alpha_proposals_mirror publishes.
    """
    if not (PREVIEW_ENABLED and ALPHA_ENABLED):
        msg = "skipped (set PREVIEW_ENABLED=1 and ALPHA_PREVIEW_PROPOSALS_ENABLED=1)"
        _log("INFO", f"alpha_proposal_runner {msg}")
        return 0, msg

    db_url = _require_db_url()

    tools_path = _sql_path(TOOLS_SQL)
    gen_path = _sql_path(GEN_SQL)
    polish_path = None
    if POLISH_ENABLED:
        try:
            polish_path = _sql_path(POLISH_SQL)
        except FileNotFoundError:
            polish_path = None  # optional

    env = os.environ.copy()
    env["PGCONNECT_TIMEOUT"] = env.get("PGCONNECT_TIMEOUT", "10")

    def _psql_scalar_int(sql: str) -> int:
        """Run a scalar SQL via psql and parse an int from stdout."""
        res = _run_cmd([PSQL_BIN, db_url, "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", sql], env=env, cwd=ROOT)
        if not res.ok:
            raise RuntimeError((res.stderr or res.stdout or "").strip() or "psql scalar query failed")
        out = (res.stdout or "").strip()
        try:
            return int(out) if out else 0
        except Exception:
            return 0

    # Snapshot size for today's UTC window (matches mirror's WHERE clause)
    count_sql = "SELECT COUNT(*) FROM alpha_proposals WHERE (ts AT TIME ZONE 'UTC')::date = (NOW() AT TIME ZONE 'UTC')::date;"
    before_total = 0
    try:
        before_total = _psql_scalar_int(count_sql)
    except Exception:
        # If table doesn't exist yet, tools SQL will create it; treat as 0.
        before_total = 0

    # 1) tools (safe preview=0)
    tmp_tools = None
    try:
        tools_sql = tools_path.read_text(encoding="utf-8", errors="replace")
        tmp_tools = _write_temp_sql(_patched_tools_sql(tools_sql))
        _log("INFO", f"alpha_proposal_runner: tools (safe) -> {tools_path.name}")
        res_tools = _run_cmd([PSQL_BIN, db_url, "-v", "ON_ERROR_STOP=1", "-f", str(tmp_tools)], env=env, cwd=ROOT)
        if not res_tools.ok:
            _log("ERROR", f"alpha_proposal_runner tools failed rc={res_tools.returncode}")
            if res_tools.stderr.strip():
                _log("ERROR", res_tools.stderr.strip())
            return 0, f"tools_failed rc={res_tools.returncode}"

        # 2) generator (preview_enabled=1)
        _log("INFO", f"alpha_proposal_runner: generator -> {gen_path.name}")
        res_gen = _run_cmd([PSQL_BIN, db_url, "-v", "ON_ERROR_STOP=1", "-v", "preview_enabled=1", "-f", str(gen_path)], env=env, cwd=ROOT)
        if not res_gen.ok:
            _log("ERROR", f"alpha_proposal_runner generator failed rc={res_gen.returncode}")
            if res_gen.stderr.strip():
                _log("ERROR", res_gen.stderr.strip())
            return 0, f"generator_failed rc={res_gen.returncode}"

        # 3) polish (optional)
        if polish_path is not None:
            _log("INFO", f"alpha_proposal_runner: polish -> {polish_path.name}")
            res_polish = _run_cmd([PSQL_BIN, db_url, "-v", "ON_ERROR_STOP=1", "-v", "preview_enabled=1", "-f", str(polish_path)], env=env, cwd=ROOT)
            if not res_polish.ok:
                _log("ERROR", f"alpha_proposal_runner polish failed rc={res_polish.returncode}")
                if res_polish.stderr.strip():
                    _log("ERROR", res_polish.stderr.strip())
                return 0, f"polish_failed rc={res_polish.returncode}"

        after_total = 0
        try:
            after_total = _psql_scalar_int(count_sql)
        except Exception:
            after_total = before_total

        generated_new = max(0, after_total - before_total)

        _log("INFO", f"alpha_proposal_runner: ok generated_new={generated_new} proposals_today_total={after_total}")
        return generated_new, "ok"

    finally:
        if tmp_tools is not None:
            try:
                tmp_tools.unlink(missing_ok=True)  # py3.11+
            except Exception:
                pass


def main() -> None:
    count, status = run_alpha_proposal_runner()
    _log("INFO", f"alpha_proposal_runner finished: status={status} count={count}")


if __name__ == "__main__":
    main()
