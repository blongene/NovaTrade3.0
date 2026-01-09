#!/usr/bin/env python3
"""
alpha_proposal_runner.py (Phase 26A, SQL-native, preview-only)

Design goals (per MSD50–52 + Phase 26 runway):
- Python does NOT re-implement alpha logic.
- Python only orchestrates the canonical SQL pipeline using psql.
- SAFE DEFAULTS: does nothing unless explicitly enabled by env flags.
- Never enqueues commands / never touches outbox / never executes trades.

What this runner does:
1) Ensures alpha_tools.sql can be applied safely (preview_enabled forced to 0 in a temp copy).
2) Runs alpha_proposal_generator.sql with preview_enabled=1 (only if enabled).
3) Optionally runs alpha_polish.sql (non-destructive view/materialization polish).

This avoids the recurring redeploy loop caused by:
- gate column type drift (int vs boolean)
- CTE ordering changes (e.g., "norm" CTE)
- column renames in alpha_readiness_v

Because the SQL files are the source of truth, and psql handles their meta-commands (\set, \if).
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class AlphaRunnerConfig:
    db_url: str
    agent_id: str
    preview_enabled: int
    run_enabled: bool
    apply_tools_if_missing: bool
    run_polish: bool

    # Optional knobs passed into SQL (if scripts support them)
    default_trade_notional_usd: Optional[float] = None
    default_trade_confidence: Optional[float] = None
    default_watch_confidence: Optional[float] = None


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "t", "yes", "y", "on")


def _env_float(name: str) -> Optional[float]:
    v = (os.getenv(name) or "").strip()
    if not v:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _log(msg: str) -> None:
    # Keep logs Render-friendly (single-line, timestamp handled by logger upstream)
    print(msg, flush=True)


def _psql_cmd(db_url: str, sql_path: str, vars_map: Dict[str, str | int | float]) -> list[str]:
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-f", sql_path]
    # Important: pass vars BEFORE -f is ok; psql uses them for :'var' and :var
    # We'll add them after base but before -f for clarity (psql accepts anywhere).
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1"]
    for k, v in vars_map.items():
        cmd += ["-v", f"{k}={v}"]
    cmd += ["-f", sql_path]
    return cmd


def _run_psql(db_url: str, sql_path: str, vars_map: Dict[str, str | int | float]) -> None:
    cmd = _psql_cmd(db_url, sql_path, vars_map)
    _log(f"[alpha_runner] psql exec: {os.path.basename(sql_path)} (vars: {', '.join(sorted(vars_map.keys()))})")
    # Capture output for debugging without spamming; show on failure
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(
            f"psql failed for {sql_path} (rc={proc.returncode}).\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )


def _safe_tools_temp_copy(original_path: str) -> str:
    """
    alpha_tools.sql historically had a line that could set preview_enabled=1.
    Even if later fixed, we defensively force any '\\set preview_enabled <x>' to 0
    in a TEMP copy, so running tools cannot insert proposals unintentionally.
    """
    with open(original_path, "r", encoding="utf-8", errors="ignore") as f:
        txt = f.read()

    # Force preview_enabled to 0 if hard-set anywhere
    txt2 = re.sub(r"(?m)^\s*\\set\s+preview_enabled\s+\S+\s*$", r"\\set preview_enabled 0", txt)

    fd, tmp_path = tempfile.mkstemp(prefix="alpha_tools_safe_", suffix=".sql")
    os.close(fd)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(txt2)
    return tmp_path


def _sql_path(name: str) -> str:
    # Prefer repo-local sql/ folder; fall back to current directory
    candidates = [
        os.path.join(os.getcwd(), "sql", name),
        os.path.join(os.getcwd(), name),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"Missing SQL file: {name}. Expected at: {candidates}")


def load_config() -> AlphaRunnerConfig:
    db_url = (os.getenv("DB_URL") or os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or "").strip()
    if not db_url:
        raise RuntimeError("Missing DB connection env. Set DB_URL or DATABASE_URL.")

    # Global preview gate (match your conventions)
    preview_global = _env_bool("PREVIEW_ENABLED", default=False)
    preview_alpha = _env_bool("ALPHA_PREVIEW_PROPOSALS_ENABLED", default=False)

    run_enabled = preview_global and preview_alpha

    agent_id = (os.getenv("AGENT_ID") or os.getenv("ALPHA_AGENT_ID") or "edge-primary").strip()

    cfg = AlphaRunnerConfig(
        db_url=db_url,
        agent_id=agent_id,
        preview_enabled=1 if run_enabled else 0,
        run_enabled=run_enabled,
        apply_tools_if_missing=_env_bool("ALPHA_APPLY_TOOLS_IF_MISSING", default=True),
        run_polish=_env_bool("ALPHA_RUN_POLISH", default=True),
        default_trade_notional_usd=_env_float("ALPHA_DEFAULT_TRADE_NOTIONAL_USD"),
        default_trade_confidence=_env_float("ALPHA_DEFAULT_TRADE_CONFIDENCE"),
        default_watch_confidence=_env_float("ALPHA_DEFAULT_WATCH_CONFIDENCE"),
    )
    return cfg


def run_alpha_preview_proposals() -> None:
    cfg = load_config()

    if not cfg.run_enabled:
        _log("[alpha_runner] preview proposals disabled (set PREVIEW_ENABLED=1 and ALPHA_PREVIEW_PROPOSALS_ENABLED=1).")
        return

    # 1) Ensure tools are applied safely (creates alpha_readiness_v, alpha_proposals table, etc.)
    tools_sql = _sql_path("alpha_tools.sql")
    safe_tools = _safe_tools_temp_copy(tools_sql)

    try:
        # Always run tools in SAFE mode (preview_enabled=0) so no inserts happen from tools.
        _run_psql(
            cfg.db_url,
            safe_tools,
            {
                "preview_enabled": 0,
                "agent_id": cfg.agent_id,
                # pass optional knobs if present; harmless if unused
                **({ "default_trade_notional_usd": cfg.default_trade_notional_usd } if cfg.default_trade_notional_usd is not None else {}),
                **({ "default_trade_confidence": cfg.default_trade_confidence } if cfg.default_trade_confidence is not None else {}),
                **({ "default_watch_confidence": cfg.default_watch_confidence } if cfg.default_watch_confidence is not None else {}),
            },
        )
    finally:
        try:
            os.remove(safe_tools)
        except Exception:
            pass

    # 2) Generate proposals (this script is preview-gated internally and checks view/table presence)
    gen_sql = _sql_path("alpha_proposal_generator.sql")
    _run_psql(
        cfg.db_url,
        gen_sql,
        {
            "preview_enabled": cfg.preview_enabled,
            "agent_id": cfg.agent_id,
            # optional knobs
            **({ "default_trade_notional_usd": cfg.default_trade_notional_usd } if cfg.default_trade_notional_usd is not None else {}),
            **({ "default_trade_confidence": cfg.default_trade_confidence } if cfg.default_trade_confidence is not None else {}),
            **({ "default_watch_confidence": cfg.default_watch_confidence } if cfg.default_watch_confidence is not None else {}),
        },
    )

    # 3) Optional polish (views / dashboards; should be safe + idempotent)
    if cfg.run_polish:
        polish_sql = _sql_path("alpha_polish.sql")
        _run_psql(cfg.db_url, polish_sql, {"preview_enabled": cfg.preview_enabled, "agent_id": cfg.agent_id})

    _log("[alpha_runner] Phase 26A preview proposals run complete.")


if __name__ == "__main__":
    try:
        run_alpha_preview_proposals()
    except Exception as e:
        _log(f"[alpha_runner] ERROR: {e}")
        raise


# -----------------------------------------------------------------------------
# Compatibility shim (expected by phase26a_smoketest.py)
# -----------------------------------------------------------------------------
def run_alpha_proposal_runner():
    """Compatibility shim. Calls main runner and returns (count, status)."""
    try:
        return run(), "ok"
    except Exception as e:
        _log("ERROR", f"alpha_proposal_runner failed: {e}")
        return 0, f"error: {e}"
