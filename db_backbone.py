# db_backbone.py
"""
DB backbone helper — thin wrapper around bus_store_pg.get_store()

Goals:
- Safe, no-op if DB_URL / psycopg2 aren't available.
- Simple helpers for receipts + telemetry + basic stats.
"""

from __future__ import annotations

import traceback
from typing import Any, Dict, Optional

try:
    from bus_store_pg import get_store  # PGStore / SQLiteStore
except Exception:  # very defensive
    get_store = None  # type: ignore[assignment]

_store = None  # type: ignore[assignment]


def _get_store():
    """Return a singleton PG/SQLite store or None if unavailable."""
    global _store
    if _store is not None:
        return _store
    if not get_store:
        print("[db_backbone] bus_store_pg.get_store not available; DB backbone disabled.")
        _store = None
        return _store
    try:
        _store = get_store()
        return _store
    except Exception as e:
        print(f"[db_backbone] get_store() failed; DB backbone disabled: {e}")
        traceback.print_exc()
        _store = None
        return _store


def record_receipt(agent_id: str, cmd_id: Optional[int], receipt: Dict[str, Any], ok: bool = True) -> None:
    """
    Fire-and-forget logging of a normalized receipt into Postgres.
    Does *not* affect command state; still handled by outbox_db.
    """
    store = _get_store()
    if not store or not hasattr(store, "save_receipt"):
        return
    try:
        store.save_receipt(agent_id, cmd_id, receipt, ok=ok)
    except Exception as e:
        print(f"[db_backbone] save_receipt failed: {e}")
        traceback.print_exc()


def record_telemetry(agent_id: str, payload: Dict[str, Any]) -> None:
    """
    Fire-and-forget logging of telemetry payloads (balances, snapshots, etc.).
    """
    store = _get_store()
    if not store or not hasattr(store, "save_telemetry"):
        return
    try:
        store.save_telemetry(agent_id, payload)
    except Exception as e:
        print(f"[db_backbone] save_telemetry failed: {e}")
        traceback.print_exc()


def outbox_stats() -> Dict[str, Any]:
    """
    Basic stats: {"queued": ..., "leased": ..., "done": ...}
    Useful for health checks / Nova Daily.
    """
    store = _get_store()
    if not store or not hasattr(store, "stats"):
        return {}
    try:
        return store.stats() or {}
    except Exception as e:
        print(f"[db_backbone] stats() failed: {e}")
        traceback.print_exc()
        return {}
