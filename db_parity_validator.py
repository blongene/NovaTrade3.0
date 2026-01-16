# db_parity_validator.py — Phase 22B (Parity Validator)
#
# Purpose
# -------
# Confirm DB shadow-write streams remain live and roughly aligned with Sheets mirrors.
#
# Canon rules
# -----------
# - Advisory only: never blocks, never crashes, never spams.
# - Any DB/Sheets error must degrade silently.
#
# Controlled by DB_READ_JSON (top-level `parity` key):
#   {
#     "parity": {"enabled":1,"window_h":24,"notify":0,"log_policy":1,"max_rows":5000}
#   }

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _load_cfg() -> Dict[str, Any]:
    """Parse DB_READ_JSON robustly (copy/paste tolerant)."""
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    # normal
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, list):
            merged: Dict[str, Any] = {}
            for part in obj:
                if isinstance(part, dict):
                    merged.update(part)
            return merged
        return {}
    except Exception:
        pass
    # common: two dicts pasted separated by comma
    try:
        obj2 = json.loads("[" + raw + "]")
        if isinstance(obj2, list):
            merged: Dict[str, Any] = {}
            for part in obj2:
                if isinstance(part, dict):
                    merged.update(part)
            return merged
    except Exception:
        pass
    # last resort: first object span
    try:
        a = raw.find("{")
        b = raw.rfind("}")
        if a != -1 and b != -1 and b > a:
            obj3 = json.loads(raw[a : b + 1])
            return obj3 if isinstance(obj3, dict) else {}
    except Exception:
        pass
    return {}


def _cfg_int(d: Dict[str, Any], k: str, default: int) -> int:
    try:
        return int(d.get(k, default))
    except Exception:
        return default


def _fmt_age(a: Optional[float]) -> str:
    if a is None:
        return "—"
    if a < 120:
        return f"{int(a)}s"
    if a < 7200:
        return f"{int(a // 60)}m"
    return f"{int(a // 3600)}h"


def _get_sheet_latest_ts(tab: str) -> Optional[float]:
    """Best-effort: read newest timestamp from the last row of a Sheet tab."""
    try:
        from utils import get_ws

        ws = get_ws(tab)
        vals = ws.get_all_values()
        if not vals or len(vals) < 2:
            return None
        header = [h.strip() for h in vals[0]]
        last = vals[-1]
        idx = None
        for cand in ("Timestamp", "timestamp", "Created_At", "created_at", "ts"):
            if cand in header:
                idx = header.index(cand)
                break
        if idx is None or idx >= len(last):
            return None
        raw = str(last[idx]).strip()
        if not raw:
            return None
        try:
            return float(raw)
        except Exception:
            pass
        import datetime as _dt

        dt = _dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _db_latest_ts(logical_stream: str) -> Optional[float]:
    """Use db_read_adapter's internal freshness probe (advisory-only)."""
    try:
        from db_read_adapter import _choose_table, _max_created_at  # type: ignore

        base = (logical_stream or "").strip().lower()
        table = _choose_table(base)
        if not table:
            return None
        return _max_created_at(table)
    except Exception:
        return None


def _db_count(logical_stream: str, limit: int) -> int:
    """Best-effort row count proxy via a limited fetch (cheap + safe)."""
    try:
        from db_read_adapter import _choose_table, _fetch_table_rows  # type: ignore

        table = _choose_table((logical_stream or "").strip().lower())
        if not table:
            return 0
        rows = _fetch_table_rows(table, limit=limit)
        return len(rows or [])
    except Exception:
        return 0


def run_db_parity_validator() -> None:
    cfg_all = _load_cfg()
    cfg = (cfg_all.get("parity") if isinstance(cfg_all, dict) else {}) or {}
    if not _truthy(cfg.get("enabled", False)):
        return

    max_rows = _cfg_int(cfg, "max_rows", 5000)
    notify = _truthy(cfg.get("notify", False))
    log_policy = _truthy(cfg.get("log_policy", False))

    checks = [
        ("telemetry", os.getenv("TELEMETRY_LOG_TAB", "Telemetry_Log")),
        ("receipts", os.getenv("TRADE_LOG_WS", "Trade_Log")),
        ("commands", os.getenv("COMMANDS_LOG_TAB", "Command_Log")),
    ]

    lines = []
    ok = True

    for stream, tab in checks:
        try:
            db_n = _db_count(stream, limit=min(max_rows, 500))
            db_ts = _db_latest_ts(stream)
            sh_ts = _get_sheet_latest_ts(tab)
            age_db = None if db_ts is None else max(0.0, time.time() - db_ts)
            age_sh = None if sh_ts is None else max(0.0, time.time() - sh_ts)

            status = "✓"
            if db_n == 0 and db_ts is None:
                # cold start / not writing yet: informational only
                status = "◌"

            # If sheets is much newer than DB, drift indicator
            if db_ts is not None and sh_ts is not None:
                if (sh_ts - db_ts) > 600:  # 10 minutes behind
                    status = "⚠"
                    ok = False

            lines.append(
                f"{stream}:{status} db_rows~={db_n} db_age={_fmt_age(age_db)} sheet_age={_fmt_age(age_sh)} tab={tab}"
            )
        except Exception as e:
            ok = False
            lines.append(f"{stream}:✗ error={e}")

    msg = "DB Parity (22B) " + ("OK" if ok else "CHECK") + " | " + " • ".join(lines)

    if log_policy:
        try:
            from utils import append_policy_log

            append_policy_log("DB_PARITY", msg)
        except Exception:
            pass

    if notify:
        try:
            from utils import send_telegram

            send_telegram(msg)
        except Exception:
            pass
