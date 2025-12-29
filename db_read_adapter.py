# db_read_adapter.py
"""
Phase 22B — DB Read Adapter (Postgres-first, Sheets-fallback) — DB_READ_JSON edition

This file is designed to be "bullet-proof":
- If Postgres/driver is missing, or any query fails -> silently fall back to Sheets.
- Uses one config env var: DB_READ_JSON (but supports legacy DB_READ_* envs as fallback).
- Supports these logical streams:
    - "commands"
    - "receipts"
    - "telemetry"
    - "sheet_mirror" (raw events)
    - "sheet_mirror:<TAB_NAME>" (returns reconstructed row dicts from payload, most recent first)

Important:
- Sheets remain primary. DB reads are advisory until parity is proven.
"""

from __future__ import annotations

import os
import json
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import logging
logger = logging.getLogger(__name__)

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None

# ----------------- config -----------------

def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _load_json_cfg() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}

_CFG = _load_json_cfg()

def _cfg_get(path: str, default=None):
    cur = _CFG
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
    return default if cur is None else cur

# DB_READ_JSON first, legacy env fallback second
DB_READ_ENABLED = bool(_cfg_get("enabled", _env_bool("DB_READ_ENABLED", "0")))
DB_READ_PREFER  = bool(_cfg_get("prefer_db", _env_bool("DB_READ_PREFER", "1")))

DB_READ_TTL_S    = int(_cfg_get("ttl_s", os.getenv("DB_READ_TTL_S", "120") or "120"))
DB_READ_MAX_ROWS = int(_cfg_get("max_rows", os.getenv("DB_READ_MAX_ROWS", "2000") or "2000"))
DB_READ_STALE_SEC = int(_cfg_get("stale_sec", os.getenv("DB_READ_STALE_SEC", "900") or "900"))

_DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL") or ""

# ----------------- internal cache -----------------

_cache_lock = threading.Lock()
_cache: Dict[str, Tuple[float, Any]] = {}  # key -> (expires_at, value)

def _cache_get(key: str) -> Optional[Any]:
    with _cache_lock:
        item = _cache.get(key)
        if not item:
            return None
        exp, val = item
        if time.time() < exp:
            return val
        _cache.pop(key, None)
        return None

def _cache_set(key: str, val: Any, ttl_s: int) -> None:
    with _cache_lock:
        _cache[key] = (time.time() + ttl_s, val)

# ----------------- PG client -----------------

@dataclass
class PgHealth:
    ok: bool
    url_set: bool
    driver_ok: bool
    last_err: Optional[str] = None

class PgClient:
    def __init__(self) -> None:
        self._conn = None
        self._lock = threading.Lock()
        self._last_err: Optional[str] = None

    def health(self) -> PgHealth:
        return PgHealth(
            ok=bool(self._conn) and self._last_err is None,
            url_set=bool(_DB_URL),
            driver_ok=psycopg2 is not None,
            last_err=self._last_err,
        )

    def _connect(self):
        if not (_DB_URL and psycopg2):
            return None
        try:
            c = psycopg2.connect(_DB_URL)
            c.autocommit = True
            return c
        except Exception as e:
            self._last_err = f"{type(e).__name__}: {e}"
            return None

    def conn(self):
        if not DB_READ_ENABLED:
            return None
        with self._lock:
            if self._conn is None:
                self._conn = self._connect()
            return self._conn

    def query(self, sql: str, args: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        c = self.conn()
        if not c:
            return []
        try:
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, args)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            self._last_err = f"{type(e).__name__}: {e}"
            # degrade by resetting connection to allow recovery next call
            with self._lock:
                try:
                    if self._conn:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
            return []

_pg = PgClient()

# ----------------- table detection -----------------

def _table_exists(table: str) -> bool:
    rows = _pg.query(
        "select 1 as ok from information_schema.tables where table_schema='public' and table_name=%s limit 1",
        (table,),
    )
    return bool(rows)

def _max_created_at(table: str, created_col: str = "created_at", tab: Optional[str] = None) -> Optional[float]:
    if tab and table == "sheet_mirror_events":
        rows = _pg.query(
            f"select extract(epoch from max({created_col})) as ts from {table} where tab=%s",
            (tab,),
        )
    else:
        rows = _pg.query(f"select extract(epoch from max({created_col})) as ts from {table}")
    if not rows:
        return None
    ts = rows[0].get("ts")
    try:
        return float(ts) if ts is not None else None
    except Exception:
        return None

def _parse_logical(logical: str) -> Tuple[str, Optional[str]]:
    """
    Supports:
      - "sheet_mirror:<TAB>"
    Returns (base_logical, tab_name_or_none)
    """
    logical = (logical or "").strip()
    if logical.lower().startswith("sheet_mirror:"):
        return "sheet_mirror", logical.split(":", 1)[1].strip()
    return logical.strip().lower(), None

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

    if base_logical and _table_exists(base_logical):
        return base_logical

    return None

# ----------------- public API -----------------

def db_health() -> dict:
    h = _pg.health()
    return {"enabled": DB_READ_ENABLED, "prefer": DB_READ_PREFER, "ttl_s": DB_READ_TTL_S, "stale_sec": DB_READ_STALE_SEC, "max_rows": DB_READ_MAX_ROWS, **h.__dict__}

def get_records_prefer_db(
    sheet_tab: str,
    logical_stream: str,
    ttl_s: int | None = None,
    *,
    sheets_fallback_fn=None,
) -> List[Dict[str, Any]]:
    """
    Returns rows as list[dict].

    - Prefer DB if enabled + prefer_db and table exists and freshness <= stale_sec
    - Otherwise return Sheets via sheets_fallback_fn (required)
    """
    ttl_s = DB_READ_TTL_S if ttl_s is None else int(ttl_s)

    if sheets_fallback_fn is None:
        raise ValueError("sheets_fallback_fn is required (e.g., utils.get_all_records_cached)")

    if not DB_READ_ENABLED or not DB_READ_PREFER:
        logger.info("🟡 DB READ FALLBACK [%s] reason=disabled", logical_stream)
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    base_logical, tab = _parse_logical(logical_stream)
    table = _choose_table(base_logical)
    if not table:
        logger.info("🟡 DB READ FALLBACK [%s] reason=no_table_mapping", logical_stream)
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    cache_key = f"db::{table}::{tab or ''}::{ttl_s}"
    cached = _cache_get(cache_key)
    if cached is not None:
        logger.info("🟢 DB READ HIT [%s] source=cache rows=%s", logical_stream, len(cached))
        return cached
    
    max_ts = _max_created_at(table, tab=tab)
    if max_ts is not None:
        age = time.time() - max_ts
        if age > DB_READ_STALE_SEC:
            logger.info(
                "🟡 DB READ FALLBACK [%s] reason=stale age=%.1fs stale_sec=%s table=%s tab=%s",
                logical_stream, age, DB_READ_STALE_SEC, table, tab
            )
            return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)
        else:
            logger.info(
                "ℹ️ DB READ CANDIDATE [%s] freshness_ok age=%.1fs table=%s tab=%s",
                logical_stream, age, table, tab
            )
    else:
        logger.info(
            "ℹ️ DB READ CANDIDATE [%s] no_created_at table=%s tab=%s",
            logical_stream, table, tab
        )

    rows: List[Dict[str, Any]] = _fetch_table_rows(table, limit=DB_READ_MAX_ROWS, tab=tab)
    if rows:
        _cache_set(cache_key, rows, ttl_s)
        logger.info(
            "🟢 DB READ HIT [%s] source=db rows=%s table=%s tab=%s",
            logical_stream, len(rows), table, tab
        )
        return rows
    
    logger.info(
        "🟡 DB READ FALLBACK [%s] reason=empty_result table=%s tab=%s",
        logical_stream, table, tab
    )
    return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

# ----------------- table fetchers -----------------

def _fetch_table_rows(table: str, limit: int = 2000, tab: Optional[str] = None) -> List[Dict[str, Any]]:
    limit = max(1, int(limit))

    if table == "commands":
        sql = """
          select id, created_at, agent_id, intent, intent_hash, status, leased_by, lease_expires_at, attempts, dedup_ttl_seconds
          from commands
          order by id desc
          limit %s
        """
        return _pg.query(sql, (limit,))

    if table == "receipts":
        rows = _pg.query(
            """
            select id, created_at, agent_id,
                   cmd_id,
                   coalesce(receipt, raw_payload, payload)::jsonb as payload,
                   ok, status, venue, symbol, base, quote, notional_usd
            from receipts
            order by id desc
            limit %s
            """,
            (limit,),
        )
        if rows:
            return rows
        return _pg.query(
            """
            select id, created_at, agent_id, cmd_id, payload, ok
            from nova_receipts
            order by id desc
            limit %s
            """,
            (limit,),
        )

    if table == "nova_commands":
        return _pg.query(
            """
            select id, created_at, agent_id, payload
            from nova_commands
            order by id desc
            limit %s
            """,
            (limit,),
        )

    if table == "nova_receipts":
        return _pg.query(
            """
            select id, created_at, agent_id, cmd_id, ok, status, venue, symbol, base, quote, notional_usd, payload
            from nova_receipts
            order by id desc
            limit %s
            """,
            (limit,),
        )

    if table == "nova_telemetry":
        return _pg.query(
            """
            select id, created_at, agent_id, kind, payload
            from nova_telemetry
            order by id desc
            limit %s
            """,
            (limit,),
        )

    if table == "sheet_mirror_events":
        if tab:
            # Return reconstructed row dicts (payload), newest-first.
            rows = _pg.query(
                """
                select created_at, payload
                from sheet_mirror_events
                where tab=%s
                order by id desc
                limit %s
                """,
                (tab, limit),
            )
            out: List[Dict[str, Any]] = []
            for r in rows:
                p = r.get("payload")
                if isinstance(p, dict):
                    out.append(p)
                else:
                    # If payload is not dict, wrap it safely
                    out.append({"payload": p, "_created_at": str(r.get("created_at") or "")})
            return out

        # Raw event rows
        return _pg.query(
            """
            select id, created_at, tab, row_hash, payload
            from sheet_mirror_events
            order by id desc
            limit %s
            """,
            (limit,),
        )

    return _pg.query(f"select * from {table} order by 1 desc limit %s", (limit,))
