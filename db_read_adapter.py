# db_read_adapter.py
"""
Phase 22B — DB Read Adapter (DB_READ_JSON edition, Sheet Mirror aware)

What this does
--------------
- Prefer Postgres reads when enabled & fresh.
- ALWAYS fall back to Google Sheets safely (never break NovaTrade).
- Supports logical streams:
    - commands / receipts / telemetry
    - sheet_mirror
    - sheet_mirror:<TAB_NAME>  -> returns reconstructed row dicts (most recent first)

Design rules (canon)
-------------------
- Sheets remain primary.
- DB reads are advisory until parity is proven.
- Any DB failure must degrade silently to Sheets.

Configuration
-------------
Single JSON env var (preferred):
  DB_READ_JSON={"enabled":1,"prefer_db":1,"ttl_s":120,"stale_sec":900,"max_rows":5000,"notify":1,...}

DB url:
  DB_URL or DATABASE_URL
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None


# ----------------- config helpers -----------------

def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _load_db_read_json() -> Dict[str, Any]:
    raw = os.getenv("DB_READ_JSON", "") or ""
    raw = raw.strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


_CFG = _load_db_read_json()


def _cfg_get(key: str, default=None):
    # simple top-level getter (we keep it basic on purpose)
    v = _CFG.get(key) if isinstance(_CFG, dict) else None
    return default if v is None else v


DB_READ_ENABLED = bool(_cfg_get("enabled", _env_bool("DB_READ_ENABLED", "0")))
DB_READ_PREFER  = bool(_cfg_get("prefer_db", _env_bool("DB_READ_PREFER", "1")))
DB_READ_TTL_S   = int(_cfg_get("ttl_s", os.getenv("DB_READ_TTL_S", "120") or "120"))
DB_READ_MAX_ROWS = int(_cfg_get("max_rows", os.getenv("DB_READ_MAX_ROWS", "2000") or "2000"))
DB_READ_STALE_SEC = int(_cfg_get("stale_sec", os.getenv("DB_READ_STALE_SEC", "900") or "900"))

_DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL") or ""


# ----------------- tiny TTL cache -----------------

_CACHE: Dict[str, Tuple[float, Any]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_get(key: str):
    now = time.time()
    with _CACHE_LOCK:
        item = _CACHE.get(key)
        if not item:
            return None
        exp, val = item
        if exp <= now:
            _CACHE.pop(key, None)
            return None
        return val


def _cache_set(key: str, val: Any, ttl_s: int):
    exp = time.time() + max(1, int(ttl_s))
    with _CACHE_LOCK:
        _CACHE[key] = (exp, val)


# ----------------- postgres wrapper -----------------

class _PG:
    def __init__(self):
        self._conn = None
        self._lock = threading.Lock()

    def _connect(self):
        if psycopg2 is None:
            return None
        if not _DB_URL:
            return None
        try:
            return psycopg2.connect(_DB_URL)
        except Exception:
            return None

    def _get_conn(self):
        with self._lock:
            if self._conn is None:
                self._conn = self._connect()
            return self._conn

    def query(self, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        if conn is None:
            return []
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:  # type: ignore
                cur.execute(sql, params)
                rows = cur.fetchall()
                return [dict(r) for r in rows] if rows else []
        except Exception:
            # advisory-only: never throw
            return []

    def scalar(self, sql: str, params: Tuple[Any, ...] = ()) -> Any:
        rows = self.query(sql, params)
        if not rows:
            return None
        # first col of first row
        return list(rows[0].values())[0]


_pg = _PG()


def _table_exists(name: str) -> bool:
    if not name:
        return False
    # safe parameterization
    return bool(
        _pg.scalar(
            "select 1 from information_schema.tables where table_schema='public' and table_name=%s limit 1",
            (name,),
        )
    )


def _parse_logical(logical_stream: str) -> Tuple[str, Optional[str]]:
    """
    Supports:
      sheet_mirror
      sheet_mirror:<TAB>
    """
    s = (logical_stream or "").strip()
    if ":" in s:
        base, tab = s.split(":", 1)
        return base.strip().lower(), tab.strip()
    return s.strip().lower(), None


def _max_created_at(table: str, tab: Optional[str] = None) -> Optional[float]:
    if not table:
        return None
    try:
        if tab and table == "sheet_mirror_events":
            sql = "select extract(epoch from max(created_at)) as ts from sheet_mirror_events where tab=%s"
            rows = _pg.query(sql, (tab,))
        else:
            sql = f"select extract(epoch from max(created_at)) as ts from {table}"
            rows = _pg.query(sql)
        if not rows:
            return None
        ts = rows[0].get("ts")
        return float(ts) if ts is not None else None
    except Exception:
        return None


def _choose_table(base_logical: str) -> Optional[str]:
    base_logical = (base_logical or "").strip().lower()

    if base_logical == "commands":
        if _table_exists("commands"):
            return "commands"
        if _table_exists("nova_commands"):
            return "nova_commands"
        return None

    if base_logical == "receipts":
        if _table_exists("receipts"):
            return "receipts"
        if _table_exists("nova_receipts"):
            return "nova_receipts"
        return None

    if base_logical == "telemetry":
        if _table_exists("nova_telemetry"):
            return "nova_telemetry"
        return None

    if base_logical == "sheet_mirror":
        if _table_exists("sheet_mirror_events"):
            return "sheet_mirror_events"
        return None

    # allow direct table name
    if base_logical and _table_exists(base_logical):
        return base_logical

    return None


def _fetch_table_rows(table: str, limit: int = 2000, tab: Optional[str] = None) -> List[Dict[str, Any]]:
    limit = max(1, int(limit))

    if table == "sheet_mirror_events":
        if tab:
            return _pg.query(
                """
                select created_at, tab, row_hash, payload
                from sheet_mirror_events
                where tab=%s
                order by created_at desc
                limit %s
                """,
                (tab, limit),
            )
        return _pg.query(
            """
            select created_at, tab, row_hash, payload
            from sheet_mirror_events
            order by created_at desc
            limit %s
            """,
            (limit,),
        )

    # generic fallthrough (safe-ish)
    try:
        return _pg.query(f"select * from {table} order by 1 desc limit %s", (limit,))
    except Exception:
        return []


def _reconstruct_sheet_rows(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert sheet_mirror_events payloads into row dicts.
    db_mirror writes payloads like:
      {"type":"sheet_row","tab":"Wallet_Monitor","row":{...}}
      {"type":"append","tab":"Trade_Log","row":{...}}
    """
    out: List[Dict[str, Any]] = []
    for ev in events:
        payload = ev.get("payload")
        if payload is None:
            continue
        # psycopg2 may return dict for JSONB; but handle strings too
        try:
            if isinstance(payload, str):
                payload_obj = json.loads(payload)
            else:
                payload_obj = payload
        except Exception:
            continue

        if isinstance(payload_obj, dict):
            row = payload_obj.get("row")
            if isinstance(row, dict):
                out.append(row)
                continue
            # allow payload itself to be row-like
            # (useful if future mirror schema changes)
            if "type" not in payload_obj and "tab" not in payload_obj:
                out.append(payload_obj)
                continue
        # ignore unknown shapes
    return out


# ----------------- public API -----------------

def db_health() -> Dict[str, Any]:
    return {
        "enabled": bool(DB_READ_ENABLED),
        "prefer_db": bool(DB_READ_PREFER),
        "has_driver": psycopg2 is not None,
        "db_url": bool(_DB_URL),
        "tables": {
            "sheet_mirror_events": _table_exists("sheet_mirror_events"),
            "nova_telemetry": _table_exists("nova_telemetry"),
            "nova_receipts": _table_exists("nova_receipts"),
            "nova_commands": _table_exists("nova_commands"),
            "commands": _table_exists("commands"),
            "receipts": _table_exists("receipts"),
        },
    }


def get_records_prefer_db(
    sheet_tab: str,
    logical_stream: str,
    ttl_s: int | None = None,
    *,
    sheets_fallback_fn=None,
) -> List[Dict[str, Any]]:
    """
    Returns rows as list[dict].

    - Prefer DB if:
        * DB_READ_ENABLED and DB_READ_PREFER
        * table exists
        * and freshness within DB_READ_STALE_SEC
    - Otherwise: Sheets fallback (required)

    Special:
      logical_stream="sheet_mirror:<TAB>" returns reconstructed sheet row dicts.
    """
    ttl_s = DB_READ_TTL_S if ttl_s is None else int(ttl_s)

    if sheets_fallback_fn is None:
        raise ValueError("sheets_fallback_fn is required (e.g., utils.get_all_records_cached)")

    if not DB_READ_ENABLED or not DB_READ_PREFER:
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    base, tab = _parse_logical(logical_stream)
    table = _choose_table(base)
    if not table:
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    cache_key = f"db::{table}::{tab or ''}::{ttl_s}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    max_ts = _max_created_at(table, tab=tab if table == "sheet_mirror_events" else None)
    if max_ts is not None:
        age = time.time() - max_ts
        if age > DB_READ_STALE_SEC:
            return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    events = _fetch_table_rows(table, limit=DB_READ_MAX_ROWS, tab=tab if table == "sheet_mirror_events" else None)
    if not events:
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    # sheet_mirror:<TAB> => reconstruct to row dicts
    if base == "sheet_mirror" and tab:
        rows = _reconstruct_sheet_rows(events)
        if rows:
            _cache_set(cache_key, rows, ttl_s)
            return rows
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    # raw table read
    _cache_set(cache_key, events, ttl_s)
    return events


# convenience export expected by some patches
def get_sheet_mirror_rows(tab: str, *, ttl_s: int = 120, sheets_fallback_fn=None) -> List[Dict[str, Any]]:
    return get_records_prefer_db(
        sheet_tab=tab,
        logical_stream=f"sheet_mirror:{tab}",
        ttl_s=ttl_s,
        sheets_fallback_fn=sheets_fallback_fn or (lambda sheet_tab, ttl_s=120: []),
    )
