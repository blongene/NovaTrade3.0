# db_parity_validator.py — Phase 22B (parity validation)
#
# Purpose:
#   - Confirm DB shadow-write streams remain live and roughly aligned with Sheets mirrors
#   - Best-effort only: never blocks, never crashes, never spams
#
# Controlled by DB_READ_JSON:
#   {
#     "parity": {"enabled":1,"window_h":24,"notify":1,"log_policy":1,"max_rows":5000}
#   }

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, Tuple

def _load_cfg() -> Dict[str, Any]:
    raw = os.getenv("DB_READ_JSON", "") or ""
    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}

def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1","true","yes","y","on")

def _cfg_int(d: Dict[str, Any], k: str, default: int) -> int:
    try:
        return int(d.get(k, default))
    except Exception:
        return default

def _get_sheet_latest_ts(tab: str) -> Optional[float]:
    """Best-effort: read the newest timestamp from the end of a Sheet tab.

    We avoid get_all_records (expensive); we read only the last row via gspread.
    Handles common timestamp columns:
      - Timestamp, timestamp, Created_At, created_at, ts
    """
    try:
        from utils import get_ws
        ws = get_ws(tab)
        vals = ws.get_all_values()
        if not vals or len(vals) < 2:
            return None
        header = [h.strip() for h in vals[0]]
        last = vals[-1]
        # locate timestamp-like col
        idx = None
        for cand in ("Timestamp","timestamp","Created_At","created_at","ts"):
            if cand in header:
                idx = header.index(cand)
                break
        if idx is None or idx >= len(last):
            return None
        raw = str(last[idx]).strip()
        if not raw:
            return None
        # numeric?
        try:
            return float(raw)
        except Exception:
            pass
        # ISO parse
        import datetime as _dt
        dt = _dt.datetime.fromisoformat(raw.replace("Z","+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def _db_latest_ts(stream: str) -> Optional[float]:
    try:
        from db_read_adapter import _db_recent  # type: ignore
        rows = _db_recent(stream, limit=1)
        if not rows:
            return None
        # try keys
        r = rows[0]
        for k in ("ts","timestamp","created_ts","created_at"):
            v = r.get(k)
            if v is None:
                continue
            try:
                return float(v)
            except Exception:
                pass
            try:
                import datetime as _dt
                dt = _dt.datetime.fromisoformat(str(v).replace("Z","+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_dt.timezone.utc)
                return dt.timestamp()
            except Exception:
                continue
        return None
    except Exception:
        return None

def run_db_parity_validator() -> None:
    cfg = _load_cfg().get("parity", {}) or {}
    if not _truthy(cfg.get("enabled", False)):
        return

    max_rows = _cfg_int(cfg, "max_rows", 5000)
    notify = _truthy(cfg.get("notify", False))
    log_policy = _truthy(cfg.get("log_policy", False))

    # Map DB streams -> the Sheet mirror tab we expect to be related.
    # These are best-effort and safe if the tab doesn't exist.
    checks = [
        ("telemetry", os.getenv("TELEMETRY_LOG_TAB", "Telemetry_Log")),
        ("receipts",  os.getenv("TRADE_LOG_WS", "Trade_Log")),
        ("commands",  os.getenv("COMMANDS_LOG_TAB", "Command_Log")),
    ]

    lines = []
    ok = True

    for stream, tab in checks:
        try:
            from db_read_adapter import _db_recent  # type: ignore
            db_rows = _db_recent(stream, limit=max_rows)
            db_n = len(db_rows)
            db_ts = _db_latest_ts(stream)
            sh_ts = _get_sheet_latest_ts(tab)
            age_db = None if db_ts is None else max(0.0, time.time() - db_ts)
            age_sh = None if sh_ts is None else max(0.0, time.time() - sh_ts)

            status = "✓"
            # if DB has nothing, flag but do not fail hard (cold start)
            if db_n == 0:
                status = "◌"
            # if sheets latest is much newer than DB, drift indicator
            if db_ts is not None and sh_ts is not None:
                if (sh_ts - db_ts) > 600:  # 10 minutes behind
                    status = "⚠"
                    ok = False

            def fmt_age(a):
                if a is None:
                    return "—"
                if a < 120:
                    return f"{int(a)}s"
                if a < 7200:
                    return f"{int(a//60)}m"
                return f"{int(a//3600)}h"

            lines.append(
                f"{stream}:{status} db_rows={db_n} db_age={fmt_age(age_db)} sheet_age={fmt_age(age_sh)} tab={tab}"
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
