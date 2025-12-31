"""
parity_auto_heal.py — Phase 23 / Module 11

Optional parity "auto-heal" for DB mirror drift, OFF by default.

What it does
- When the parity validator detects drift for a tab, this module can (optionally)
  perform a one-shot "heal" by mirroring the current Sheets rows into Postgres
  via db_mirror.mirror_rows(tab, rows).

Safety
- OFF by default (requires DB_READ_JSON.parity.auto_heal == 1/true).
- Once-per-tab-per-window guard (default window = parity.window_h or 24h).
- Best-effort: if DB is down or any exception occurs, it NO-OPs silently.
- It never blocks the parity validator or breaks Sheets-primary behavior.

Config (DB_READ_JSON)
{
  "parity": {
    "enabled": 1,
    "window_h": 24,
    "notify": 1,
    "auto_heal": 0,
    "auto_heal_tabs": ["Trade_Log","Wallet_Monitor"]   # optional allowlist
  }
}

Notes
- The "heal" operation is simply adding a fresh mirror snapshot to DB.
  It does NOT modify Google Sheets.
"""
from __future__ import annotations

import os
import json
import time
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

__all__ = ["maybe_auto_heal"]

# In-process fallback guard (used only if DB guard isn't available)
_LOCAL_GUARD: Dict[str, float] = {}

# One-time log guards (per process) to avoid spam
_LOGGED_ENABLED = False
_LOGGED_DB_GUARD_FAIL = False


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _load_db_read_json() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _cfg() -> dict:
    return _load_db_read_json() or {}


def _parity_cfg() -> dict:
    c = _cfg()
    p = c.get("parity") or {}
    if isinstance(p, dict):
        return p
    return {}


def _auto_heal_enabled() -> bool:
    p = _parity_cfg()
    return _truthy(p.get("auto_heal", 0))


def _auto_heal_tabs_allowlist() -> Optional[List[str]]:
    p = _parity_cfg()
    tabs = p.get("auto_heal_tabs")
    if not tabs:
        return None
    if isinstance(tabs, str):
        parts = [t.strip() for t in tabs.split(",") if t.strip()]
        return parts or None
    if isinstance(tabs, list):
        out = []
        for t in tabs:
            s = str(t).strip()
            if s:
                out.append(s)
        return out or None
    return None


def _window_h() -> int:
    p = _parity_cfg()
    try:
        return int(p.get("window_h", 24) or 24)
    except Exception:
        return 24


def _window_key(now: Optional[float] = None) -> str:
    now = float(now or time.time())
    wh = max(1, _window_h())
    bucket = int(now // float(wh * 3600))
    return f"{bucket}"


def _db_guard_claim(tab: str, window_key: str) -> bool:
    """Return True if we successfully claim heal for (tab, window)."""
    global _LOGGED_DB_GUARD_FAIL
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        return False
    try:
        import psycopg2  # type: ignore
    except Exception:
        return False

    try:
        conn = psycopg2.connect(db_url, connect_timeout=5)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS parity_autoheal_runs (
                  tab TEXT NOT NULL,
                  window_key TEXT NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (tab, window_key)
                );
                """
            )
            # Claim: insert; if conflict -> already claimed
            cur.execute(
                """
                INSERT INTO parity_autoheal_runs(tab, window_key)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (tab, window_key),
            )
            # rowcount == 1 means we inserted (claimed)
            claimed = bool(getattr(cur, "rowcount", 0) == 1)
        try:
            conn.close()
        except Exception:
            pass
        return claimed
    except Exception as e:
        if not _LOGGED_DB_GUARD_FAIL:
            _LOGGED_DB_GUARD_FAIL = True
            logger.info("parity_auto_heal: DB guard unavailable; falling back to local guard (%s)", e)
        return False


def _local_guard_claim(tab: str, window_key: str) -> bool:
    key = f"{tab}:{window_key}"
    now = time.time()
    # expire local guards slightly beyond the window
    ttl = max(3600, _window_h() * 3600 + 600)
    # purge stale
    stale_keys = [k for k, ts in _LOCAL_GUARD.items() if (now - ts) > ttl]
    for k in stale_keys:
        _LOCAL_GUARD.pop(k, None)
    if key in _LOCAL_GUARD:
        return False
    _LOCAL_GUARD[key] = now
    return True


def maybe_auto_heal(tab: str, sheets_rows: List[Any], drift_record: Optional[Dict[str, Any]] = None) -> bool:
    """Attempt auto-heal for tab if enabled. Returns True if healed."""
    global _LOGGED_ENABLED

    if not _auto_heal_enabled():
        return False

    if not _LOGGED_ENABLED:
        _LOGGED_ENABLED = True
        logger.info("parity_auto_heal: ENABLED (one-shot per tab per window)")

    allow = _auto_heal_tabs_allowlist()
    if allow is not None and tab not in allow:
        return False

    wk = _window_key()
    claimed = _db_guard_claim(tab, wk)
    if not claimed:
        claimed = _local_guard_claim(tab, wk)

    if not claimed:
        return False

    # Perform heal: mirror current Sheets rows into DB (best-effort)
    try:
        import db_mirror  # type: ignore
        db_mirror.mirror_rows(tab, sheets_rows)
        # single concise log on successful heal
        extra = ""
        try:
            if drift_record:
                extra = f" sheets={drift_record.get('sheets_n')} db={drift_record.get('db_n')} overlap={drift_record.get('overlap')}"
        except Exception:
            pass
        logger.info("🩹 parity_auto_heal: mirrored Sheets->DB tab=%s n=%s%s", tab, len(sheets_rows), extra)
        return True
    except Exception as e:
        # If mirror fails, do not retry within this window; we already claimed.
        logger.info("parity_auto_heal: mirror failed tab=%s err=%s", tab, e)
        return False
