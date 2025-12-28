"""db_mirror.py

Phase 22A (DB shadow-write)
--------------------------
Best-effort mirror of Google Sheets *append* operations into Postgres.

This is deliberately **shadow-only**:
  - It never replaces Sheets as the source of truth (yet)
  - It must never block or fail the main loop

We only mirror *append_row/append_rows* (append-only tabs). Other write
patterns (update/cell edits) can be added later.

Environment
-----------
DATABASE_URL or DB_URL:
    Postgres connection string.
DB_MIRROR_ENABLED:
    "1"/"true" to enable. Default: off.
DB_MIRROR_TABS:
    Comma-separated worksheet titles to mirror. Default mirrors key audit tabs.
DB_MIRROR_MAX_ROWS:
    Safety cap per call (default 500).
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from typing import Any, Iterable, List, Optional

try:
    import psycopg2
    import psycopg2.extras
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore
    psycopg2_extras = None  # type: ignore

DB_MIRROR_ENABLED = env_enabled("DB_MIRROR_ENABLED", False)

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
]


def _truthy(v: str | None) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _db_url() -> str:
    return os.getenv("DATABASE_URL") or os.getenv("DB_URL") or ""


def enabled() -> bool:
    return (
        psycopg2 is not None
        and _truthy(os.getenv("DB_MIRROR_ENABLED"))
        and bool(_db_url())
    )

def env_enabled(name: str, default=False):
    val = os.getenv(name)
    if val is None:
        return default
    return val.lower() in ("1", "true", "yes", "on")

def allowed_tabs() -> set[str]:
    raw = os.getenv("DB_MIRROR_TABS", "").strip()
    if not raw:
        return set(DEFAULT_TABS)
    return {t.strip() for t in raw.split(",") if t.strip()}


def _row_hash(tab: str, payload: Any) -> str:
    # Include tab in hash to keep uniqueness per worksheet.
    blob = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256((tab + "|" + blob).encode("utf-8")).hexdigest()


class _Mirror:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._conn = None
        self._last_init_ts = 0.0
        self._init_ok = False

    def _connect(self):
        url = _db_url()
        if not url:
            return None
        conn = psycopg2.connect(url)
        conn.autocommit = True
        return conn

    def _ensure_schema(self) -> None:
        """Create mirror table if missing.

        Runs at most once per process (and re-runs if connection drops).
        """
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
                # Don't raise. We'll retry later.
                self._init_ok = False

    def mirror_append(self, tab: str, rows: List[Any]) -> None:
        if not enabled():
            return
        if tab not in allowed_tabs():
            return

        max_rows = int(os.getenv("DB_MIRROR_MAX_ROWS", "500") or "500")
        if len(rows) > max_rows:
            rows = rows[:max_rows]

        self._ensure_schema()
        if not self._init_ok or self._conn is None:
            return

        # Prepare records.
        records = []
        for r in rows:
            payload = {"type": "append", "tab": tab, "row": r}
            records.append((tab, _row_hash(tab, payload), json.dumps(payload, default=str)))

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


_MIRROR = _Mirror()


def mirror_append(tab: str, rows: List[Any]) -> None:
    """Public helper: mirror a list of rows appended to a worksheet title."""
    _MIRROR.mirror_append(tab, rows)
