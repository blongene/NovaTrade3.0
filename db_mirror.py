"""db_mirror.py

Phase 22A–22B (DB shadow-write)
------------------------------
Best-effort mirror of Google Sheets activity into Postgres.

- Phase 22A: mirror append-only writes (append_row / append_rows)
- Phase 22B Capstone: mirror READ snapshots (get_all_records) into the same
  sheet_mirror_events ledger so DB_READ adapter can serve sheet_mirror:<TAB>.

CANON RULES
-----------
- Sheets remain primary and must keep working.
- DB operations are advisory-only; they must never block or crash NovaTrade.
- Any DB error must degrade silently.

Tables
------
sheet_mirror_events:
  id BIGSERIAL PK
  tab TEXT
  row_hash TEXT (unique per tab)
  payload JSONB
  created_at TIMESTAMPTZ

Controls
--------
Writes (Phase 22A):
  DB_MIRROR_ENABLED=1 to enable append mirroring
  DB_MIRROR_TABS=comma,separated,tabs (optional allowlist)
  DB_MIRROR_MAX_ROWS (default 500)

Reads (Phase 22B):
  Uses DB_READ_JSON when present:
    {"enabled":1, "mirror_reads":1, "mirror_max_rows":1000}
  Optional override:
    DB_MIRROR_READS_ENABLED=1
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from typing import Any, List, Optional

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore

logger = logging.getLogger(__name__)

# One-time per-boot observability
_LOGGED_TABS: set[str] = set()

# ----------------- helpers -----------------

def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on"}

def _db_url() -> str:
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or ""

def _load_db_read_json_cfg() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

DEFAULT_TABS = [
    "Policy_Log",
    "Trade_Log",
    "Wallet_Monitor",
    "Unified_Snapshot",
    "Telemetry_Log",
    "NovaTrigger_Log",
    "Webhook_Debug",
    "Scout Decisions",
    "Sentiment_Log",
    "Rebuy_Insights",
    "Why_Nothing_Happened",
    "Decision_Analytics",
]

def allowed_tabs() -> set[str]:
    raw = os.getenv("DB_MIRROR_TABS", "").strip()
    if not raw:
        return set(DEFAULT_TABS)
    return {t.strip() for t in raw.split(",") if t.strip()}

def enabled_appends() -> bool:
    return (
        psycopg2 is not None
        and _truthy(os.getenv("DB_MIRROR_ENABLED"))
        and bool(_db_url())
    )

def enabled_reads() -> bool:
    # Hard override
    if _truthy(os.getenv("DB_MIRROR_READS_ENABLED")):
        return psycopg2 is not None and bool(_db_url())

    cfg = _load_db_read_json_cfg()
    if not cfg:
        return False
    if not bool(cfg.get("enabled", 0)):
        return False
    # mirror_reads defaults to 1 when DB reads enabled
    if not bool(cfg.get("mirror_reads", 1)):
        return False
    return psycopg2 is not None and bool(_db_url())

def _row_hash(tab: str, payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256((tab + "|" + blob).encode("utf-8")).hexdigest()

# ----------------- mirror engine -----------------

class _Mirror:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._conn = None
        self._last_init_ts = 0.0
        self._init_ok = False

    def _connect(self):
        url = _db_url()
        if not url or psycopg2 is None:
            return None
        conn = psycopg2.connect(url)
        conn.autocommit = True
        return conn

    def _ensure_schema(self) -> None:
        # Create mirror table if missing.
        now = time.time()
        if self._init_ok and (now - self._last_init_ts) < 60:
            return
        with self._lock:
            now = time.time()
            if self._init_ok and (now - self._last_init_ts) < 60:
                return

            self._last_init_ts = now
            if self._conn is None:
                self._conn = self._connect()
            if self._conn is None:
                self._init_ok = False
                return

            try:
                with self._conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS sheet_mirror_events (
                          id BIGSERIAL PRIMARY KEY,
                          tab TEXT NOT NULL,
                          row_hash TEXT NOT NULL,
                          payload JSONB NOT NULL,
                          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                        CREATE UNIQUE INDEX IF NOT EXISTS uq_sheet_mirror_tab_hash
                          ON sheet_mirror_events(tab, row_hash);
                        CREATE INDEX IF NOT EXISTS ix_sheet_mirror_tab_created
                          ON sheet_mirror_events(tab, created_at DESC);
                        """
                    )
                self._init_ok = True
            except Exception:
                self._init_ok = False

    def _insert_records(self, records: list[tuple[str, str, str]]) -> None:
        if not records:
            return
        self._ensure_schema()
        if not self._init_ok or self._conn is None or psycopg2 is None:
            return

        sql = """
            INSERT INTO sheet_mirror_events(tab, row_hash, payload)
            VALUES %s
            ON CONFLICT (tab, row_hash) DO NOTHING
        """
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, records, page_size=200)
        except Exception:
            # Drop connection so next attempt reconnects.
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            self._init_ok = False

    # Phase 22A: append shadow-write
    def mirror_append(self, tab: str, rows: List[Any]) -> None:
        if not enabled_appends():
            return
        if tab not in allowed_tabs():
            return

        max_rows = int(os.getenv("DB_MIRROR_MAX_ROWS", "500") or "500")
        if len(rows) > max_rows:
            rows = rows[:max_rows]

        records: list[tuple[str, str, str]] = []
        for r in rows:
            payload = {"type": "append", "tab": tab, "row": r}
            records.append((tab, _row_hash(tab, payload), json.dumps(payload, default=str)))

        self._insert_records(records)

    # Phase 22B: READ snapshot shadow-write (row dicts)
    def mirror_rows(self, tab: str, rows: List[Any]) -> None:
        if not enabled_reads():
            return
        # Do NOT restrict by allowed_tabs for reads: we want broad coverage for DB read-adapter.
        try:
            cfg = _load_db_read_json_cfg()
            max_rows = int(cfg.get("mirror_max_rows", 1000) if isinstance(cfg, dict) else 1000)
        except Exception:
            max_rows = 1000
        max_rows = max(10, min(5000, max_rows))
        if isinstance(rows, list) and len(rows) > max_rows:
            rows = rows[:max_rows]

        records: list[tuple[str, str, str]] = []
        for r in rows:
            # r is typically dict from ws.get_all_records()
            payload = {"type": "sheet_row", "tab": tab, "row": r}
            records.append((tab, _row_hash(tab, payload), json.dumps(payload, default=str)))

        self._insert_records(records)

_MIRROR = _Mirror()

# ----------------- public helpers -----------------

def mirror_append(tab: str, rows: List[Any]) -> None:
    # mirror_append signature is stable; keep wrapper ultra-safe.
    try:
        _MIRROR.mirror_append(tab, rows)
    except Exception:
        pass

def mirror_rows(tab: str, rows: List[Any]) -> None:
    try:
        _MIRROR.mirror_rows(tab, rows)
    except Exception:
        return
    # ---- observability: log once per tab per boot ----
    try:
        global _LOGGED_TABS
        if tab not in _LOGGED_TABS:
            _LOGGED_TABS.add(tab)
            logger.info(
                "🪞 sheet_mirror_events: mirrored rows tab=%s n=%s",
                tab,
                len(rows),
            )
    except Exception:
        pass
