# db_read_adapter.py
"""
Phase 22B — DB Read Adapter (DB_READ_JSON edition)

Drop-in goals
-------------
- Prefer Postgres for reads when enabled & fresh.
- ALWAYS fall back to Google Sheets safely (never break NovaTrade).
- Keep behavior transparent, configurable, and bounded (max rows / stale age).

Configuration (single env var)
------------------------------
DB_READ_JSON (optional)
Example:
DB_READ_JSON={
  "enabled": 1,
  "prefer_db": 1,
  "ttl_s": 120,
  "max_rows": 2000,
  "stale_sec": 900
}

Back-compat (optional)
----------------------
If DB_READ_JSON is not set, legacy env vars are still honored:
DB_READ_ENABLED, DB_READ_PREFER, DB_READ_TTL_S, DB_READ_MAX_ROWS, DB_READ_STALE_SEC

DB connection string:
DB_URL or DATABASE_URL

Notes
-----
- This adapter is intentionally generic and table-flexible:
    logical_stream "commands"  -> commands / nova_commands
    logical_stream "receipts"  -> receipts / nova_receipts
    logical_stream "telemetry" -> nova_telemetry
    logical_stream "sheet_mirror" -> sheet_mirror_events
- Any DB error → silent fallback to Sheets.
"""

from __future__ import annotations

import os
import json
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Callable

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


# ----------------- config -----------------

def _env_bool(name: str, default: str = "1") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _safe_int(v: Any, default: int) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _load_db_read_json() -> Dict[str, Any]:
    raw = os.getenv("DB_READ_JSON", "") or ""
    if not raw.strip():
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


_CFG = _load_db_read_json()

# Prefer DB_READ_JSON, but keep legacy envs as fallback.
DB_READ_ENABLED = bool(_CFG.get("enabled")) if _CFG else _env_bool("DB_READ_ENABLED", "1")
DB_READ_PREFER  = bool(_CFG.get("prefer_db", True)) if _CFG else _env_bool("DB_READ_PREFER", "1")
DB_READ_TTL_S   = _safe_int(_CFG.get("ttl_s", 120), 120) if _CFG else _safe_int(os.getenv("DB_READ_TTL_S", "120"), 120)
DB_READ_MAX_ROWS = _safe_int(_CFG.get("max_rows", 2000), 2000) if _CFG else _safe_int(os.getenv("DB_READ_MAX_ROWS", "2000"), 2000)
DB_READ_STALE_SEC = _safe_int(_CFG.get("stale_sec", 900), 900) if _CFG else _safe_int(os.getenv("DB_READ_STALE_SEC", "900"), 900)

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
            cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # type: ignore
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
        return None

    if logical in ("sheet_mirror", "sheet_mirror_events"):
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
    return {
        "enabled": DB_READ_ENABLED,
        "prefer": DB_READ_PREFER,
        "ttl_s": DB_READ_TTL_S,
        "max_rows": DB_READ_MAX_ROWS,
        "stale_sec": DB_READ_STALE_SEC,
        **h.__dict__,
    }


def get_records_prefer_db(
    sheet_tab: str,
    logical_stream: str,
    ttl_s: Optional[int] = None,
    *,
    sheets_fallback_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """
    Returns rows as list[dict].

    Prefer DB if:
      - DB_READ_ENABLED and DB_READ_PREFER
      - table exists
      - newest DB row age <= DB_READ_STALE_SEC

    Otherwise returns Sheets via sheets_fallback_fn (required),
    typically: utils.get_all_records_cached(tab, ttl_s=ttl_s)

    Hard rule: ANY DB issue -> Sheets fallback.
    """
    ttl_s = DB_READ_TTL_S if ttl_s is None else _safe_int(ttl_s, DB_READ_TTL_S)

    if sheets_fallback_fn is None:
        raise ValueError("sheets_fallback_fn is required (e.g., utils.get_all_records_cached)")

    # Sheets forced
    if (not DB_READ_ENABLED) or (not DB_READ_PREFER):
        try:
            return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)  # type: ignore
        except TypeError:
            # Some call sites may use (tab, ttl_s) positional style
            return sheets_fallback_fn(sheet_tab, ttl_s)  # type: ignore

    table = _choose_table(logical_stream)
    if not table:
        try:
            return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)  # type: ignore
        except TypeError:
            return sheets_fallback_fn(sheet_tab, ttl_s)  # type: ignore

    cache_key = f"db::{table}::{ttl_s}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        max_ts = _max_created_at(table)
        if max_ts is not None:
            age = time.time() - max_ts
            if age > DB_READ_STALE_SEC:
                try:
                    return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)  # type: ignore
                except TypeError:
                    return sheets_fallback_fn(sheet_tab, ttl_s)  # type: ignore

        rows: List[Dict[str, Any]] = _fetch_table_rows(table, limit=DB_READ_MAX_ROWS)
        if rows:
            _cache_set(cache_key, rows, ttl_s)
            return rows
    except Exception:
        # hard fail-safe, never leak exception
        pass

    # no data -> fallback
    try:
        return sheets_fallback_fn(sheet_tab, ttl_s=ttl_s)  # type: ignore
    except TypeError:
        return sheets_fallback_fn(sheet_tab, ttl_s)  # type: ignore


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
        # schema varies across builds; select only known-safe columns; degrade if cols missing.
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
        # fallback to nova_receipts minimal
        return _pg.query(
            """
            select id, created_at, agent_id, cmd_id, ok, status, venue, symbol, base, quote, notional_usd, payload
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

    return _pg.query(f"select * from {table} order by 1 desc limit %s", (limit,))
