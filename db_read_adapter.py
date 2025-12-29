# db_read_adapter.py — Phase 22B (DB read-adapter)
#
# Goals:
#   - Prefer Postgres (db_backbone) for reads when enabled via DB_READ_JSON
#   - Fall back to Sheets (caller-provided) on *any* error
#   - Never block or crash the Bus loop
#
# Config:
#   DB_READ_JSON={"enabled":1,"prefer_db":1,"ttl_s":120,"stale_sec":900,"max_rows":2000,...}
#
# Notes:
#   This adapter is OPT-IN per call (logical_stream must be provided), so we do not
#   accidentally change schemas for modules that expect exact Sheets columns.

from __future__ import annotations

import json
import os
import time
from typing import Callable, Dict, List, Optional, Any

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

def _cfg_int(cfg: Dict[str, Any], k: str, default: int) -> int:
    try:
        return int(cfg.get(k, default))
    except Exception:
        return default

def _enabled(cfg: Dict[str, Any]) -> bool:
    return _truthy(cfg.get("enabled", False))

def _prefer_db(cfg: Dict[str, Any]) -> bool:
    return _truthy(cfg.get("prefer_db", True))

def _ttl_s(cfg: Dict[str, Any], fallback: int) -> int:
    return _cfg_int(cfg, "ttl_s", fallback)

def _stale_sec(cfg: Dict[str, Any]) -> int:
    return _cfg_int(cfg, "stale_sec", 900)

def _max_rows(cfg: Dict[str, Any]) -> int:
    return _cfg_int(cfg, "max_rows", 2000)

def _db_recent(logical_stream: str, limit: int) -> List[Dict[str, Any]]:
    """Read recent rows from Postgres via db_backbone getters.

    Returns a list of dicts (newest-first if available).
    """
    s = (logical_stream or "").strip().lower()

    from db_backbone import (  # type: ignore
        get_recent_commands,
        get_recent_receipts,
        get_recent_telemetry,
    )

    if s in ("commands", "nova_commands", "command"):
        return list(get_recent_commands(limit=limit) or [])
    if s in ("receipts", "nova_receipts", "receipt"):
        return list(get_recent_receipts(limit=limit) or [])
    if s in ("telemetry", "nova_telemetry", "tel"):
        return list(get_recent_telemetry(limit=limit) or [])

    # Unknown stream: return empty (forces sheets fallback)
    return []

def _age_sec_from_db_row(row: Dict[str, Any]) -> Optional[float]:
    # db_backbone uses numeric ts in some places; else ISO strings
    for k in ("ts", "timestamp", "created_ts", "created_at"):
        v = row.get(k)
        if v is None:
            continue
        try:
            return max(0.0, time.time() - float(v))
        except Exception:
            pass
        # ISO-ish fallback
        try:
            import datetime as _dt
            dt = _dt.datetime.fromisoformat(str(v).replace("Z","+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            return max(0.0, time.time() - dt.timestamp())
        except Exception:
            continue
    return None

def get_records_prefer_db(
    sheet_tab: str,
    logical_stream: str,
    ttl_s: int,
    sheets_fallback_fn: Callable[[str, int], List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """Prefer DB reads when enabled and fresh; otherwise fall back to Sheets.

    IMPORTANT: This is best-effort and never raises.
    """
    cfg = _load_cfg()
    ttl = _ttl_s(cfg, ttl_s)

    if _enabled(cfg) and _prefer_db(cfg):
        try:
            rows = _db_recent(logical_stream, limit=_max_rows(cfg))
            if rows:
                age = _age_sec_from_db_row(rows[0])
                if age is None or age <= _stale_sec(cfg):
                    return rows
        except Exception:
            # fall through to Sheets
            pass

    return sheets_fallback_fn(sheet_tab, ttl)
