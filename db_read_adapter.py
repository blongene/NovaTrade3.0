# db_read_adapter.py
"""
Phase 22B — DB Read Adapter (Postgres-first, Sheets-fallback)

Purpose
-------
Provide a safe, cache-backed read helper that prefers Postgres when available,
but NEVER breaks NovaTrade when Postgres is unavailable or stale.

Design rules (NovaTrade canon):
- Sheets remain the visible control plane and must continue working.
- Postgres read path is *advisory* until parity is proven.
- Any DB exception MUST degrade to Sheets, quietly.

Environment
-----------
DB_URL or DATABASE_URL:
  Postgres connection string.

DB_READ_ENABLED (default: 1):
  Set to 0 to force Sheets reads.

DB_READ_PREFER (default: 1):
  If 0, always read Sheets (still keeps DB health metrics).

DB_READ_TTL_S (default: 120):
  In-process cache TTL for DB query results.

DB_READ_MAX_ROWS (default: 2000):
  Upper bound for rows returned from DB for parity / dashboards.

DB_READ_STALE_SEC (default: 900):
  If the freshest DB row is older than this, prefer Sheets.

Notes
-----
This adapter is intentionally generic:
- It can read from:
    - 7C outbox tables: commands, receipts
    - Phase-19 backbone tables: nova_commands, nova_receipts, nova_telemetry
    - Sheet mirror events: sheet_mirror_events (tab/payload ledger)
- Callers provide a *logical* name and we auto-detect the best physical table.
"""

from __future__ import annotations

import os
import json
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None


# ----------------- config -----------------

def _env_bool(name: str, default: str = "1") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")


DB_READ_ENABLED = _env_bool("DB_READ_ENABLED", "1")
DB_READ_PREFER  = _env_bool("DB_READ_PREFER", "1")
DB_READ_TTL_S   = int(os.getenv("DB_READ_TTL_S", "120") or "120")
DB_READ_MAX_ROWS = int(os.getenv("DB_READ_MAX_ROWS", "2000") or "2000")
DB_READ_STALE_SEC = int(os.getenv("DB_READ_STALE_SEC", "900") or "900")

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


def _max_created_at(table: str, created_col: str = "created_at") -> Optional[float]:
    rows = _pg.query(f"select extract(epoch from max({created_col})) as ts from {table}")
    if not rows:
        return None
    ts = rows[0].get("ts")
    try:
        return float(ts) if ts is not None else None
    except Exception:
        return None


def _choose_table(logical: str) -> Optional[str]:
    """
    Map a logical stream to the best physical table.
    """
    logical = (logical or "").strip().lower()

    # 7C outbox schema (preferred for command bus)
    if logical == "commands":
        if _table_exists("commands"):
            return "commands"
        if _table_exists("nova_commands"):
            return "nova_commands"
        return None

    if logical == "receipts":
        if _table_exists("receipts"):
            return "receipts"
        if _table_exists("nova_receipts"):
            return "nova_receipts"
        return None

    if logical == "telemetry":
        if _table_exists("nova_telemetry"):
            return "nova_telemetry"
        # If only mirror exists, callers can use logical "sheet_mirror"
        return None

    if logical == "sheet_mirror":
        if _table_exists("sheet_mirror_events"):
            return "sheet_mirror_events"
        return None

    # allow direct table name
    if logical and _table_exists(logical):
        return logical

    return None


# ----------------- public API -----------------

def db_health() -> dict:
    h = _pg.health()
    return {"enabled": DB_READ_ENABLED, "prefer": DB_READ_PREFER, **h.__dict__}


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
    - Otherwise return Sheets via sheets_fallback_fn (required),
      typically utils.get_all_records_cached(tab, ttl_s)

    Note: This is designed to mirror utils.get_all_records_cached shape (dict rows).
    """
    ttl_s = DB_READ_TTL_S if ttl_s is None else int(ttl_s)

    if sheets_fallback_fn is None:
        raise ValueError("sheets_fallback_fn is required (e.g., utils.get_all_records_cached)")

    # Sheets forced
    if not DB_READ_ENABLED or not DB_READ_PREFER:
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    table = _choose_table(logical_stream)
    if not table:
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    cache_key = f"db::{table}::{ttl_s}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # freshness check
    max_ts = _max_created_at(table)
    if max_ts is not None:
        age = time.time() - max_ts
        if age > DB_READ_STALE_SEC:
            return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)

    rows: List[Dict[str, Any]] = _fetch_table_rows(table, limit=DB_READ_MAX_ROWS)
    if rows:
        _cache_set(cache_key, rows, ttl_s)
        return rows

    # no data -> fallback
    return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)


def _fetch_table_rows(table: str, limit: int = 2000) -> List[Dict[str, Any]]:
    """
    Returns a normalized list[dict] per table type.
    """
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
        # schema varies across builds; select only known-safe columns
        # receipts may include receipt jsonb or raw payload; try both.
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
        # some schemas don't have those cols -> fallback to minimal
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
        return _pg.query(
            """
            select id, created_at, tab, row_hash, payload
            from sheet_mirror_events
            order by id desc
            limit %s
            """,
            (limit,),
        )

    # direct generic
    return _pg.query(f"select * from {table} order by 1 desc limit %s", (limit,))
