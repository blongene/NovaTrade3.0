# alpha_command_outbox.py
# Phase 26/27 – DB-backed command outbox enqueue helpers
#
# Goals:
# - Self-contained (DO NOT depend on db.py existing).
# - Deterministic intent hashing for dedupe/idempotency.
# - Backward compatible with multiple calling styles seen across SuperDiscussions.
#
# Supported call styles:
#   enqueue_command(intent, agent_id="edge-primary", dedup_ttl_seconds=900)
#   enqueue_command(agent_id="edge-primary", intent=intent, dedup_ttl_seconds=900)
#   enqueue(agent_id="edge-primary", kind="order.place", payload={...}, dedup_ttl_seconds=900)
#
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import psycopg2


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _db_url() -> str:
    url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DB_URL (or DATABASE_URL) is not set")
    return url


def get_db_conn():
    """Postgres connector for NovaTrade commands outbox."""
    return psycopg2.connect(_db_url())


def _canonical_json(obj: Any) -> str:
    """Deterministic JSON string for hashing + dedupe."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_intent_hash(intent: Dict[str, Any]) -> str:
    """Stable hash used for unique constraint commands(intent_hash)."""
    s = _canonical_json(intent)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _ensure_type(intent: Dict[str, Any]) -> None:
    """Enforce DB constraint: commands.intent must have 'type'."""
    t = (intent or {}).get("type")
    if not t or not isinstance(t, str):
        raise ValueError("intent must include a non-empty string field: intent['type']")


@dataclass
class EnqueueResult:
    cmd_id: int
    intent_hash: str
    created_new: bool


def enqueue_command(*args, **kwargs) -> EnqueueResult:
    """Insert an intent into commands table with idempotency on intent_hash.

    Backward compatible argument handling.

    Canonical signature:
        enqueue_command(intent: dict, *, agent_id: str='cloud', status: str='queued', dedup_ttl_seconds: int=900)

    Also accepts legacy style:
        enqueue_command(agent_id='edge-primary', intent=intent, dedup_ttl_seconds=900)
    """

    # ---- Parse args/kwargs compatibly ----
    intent: Optional[Dict[str, Any]] = None
    agent_id: str = "cloud"
    status: str = "queued"
    dedup_ttl_seconds: int = 900

    # If first positional is a dict, treat as intent.
    if args:
        if isinstance(args[0], dict):
            intent = args[0]
            # Optional second positional agent_id
            if len(args) >= 2 and isinstance(args[1], str):
                agent_id = args[1]
        else:
            # Very old style: (agent_id, intent)
            if len(args) >= 2 and isinstance(args[0], str) and isinstance(args[1], dict):
                agent_id = args[0]
                intent = args[1]

    # Keyword overrides
    if "intent" in kwargs and isinstance(kwargs.get("intent"), dict):
        intent = kwargs.get("intent")
    if "agent_id" in kwargs and isinstance(kwargs.get("agent_id"), str):
        agent_id = kwargs.get("agent_id")
    if "status" in kwargs and isinstance(kwargs.get("status"), str):
        status = kwargs.get("status")
    if "dedup_ttl_seconds" in kwargs:
        try:
            dedup_ttl_seconds = int(kwargs.get("dedup_ttl_seconds"))
        except Exception:
            pass

    if not isinstance(intent, dict):
        raise ValueError("enqueue_command requires an intent dict")

    _ensure_type(intent)
    ih = compute_intent_hash(intent)

    sql_insert = """
        INSERT INTO commands (created_at, agent_id, intent, intent_hash, status, dedup_ttl_seconds)
        VALUES (%s, %s, %s::jsonb, %s, %s, %s)
        ON CONFLICT (intent_hash) DO NOTHING
        RETURNING id;
    """

    sql_select = "SELECT id FROM commands WHERE intent_hash = %s;"

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql_insert,
                (_now_utc(), agent_id, _canonical_json(intent), ih, status, int(dedup_ttl_seconds)),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                return EnqueueResult(cmd_id=int(row[0]), intent_hash=ih, created_new=True)

            # Already existed; fetch id
            cur.execute(sql_select, (ih,))
            row2 = cur.fetchone()
            if not row2 or row2[0] is None:
                raise RuntimeError("ON CONFLICT DO NOTHING but could not re-select existing command id")
            return EnqueueResult(cmd_id=int(row2[0]), intent_hash=ih, created_new=False)


def enqueue(*, agent_id: str, kind: str, payload: Dict[str, Any], dedup_ttl_seconds: int = 900, status: str = "queued") -> EnqueueResult:
    """Legacy convenience wrapper: enqueue(kind,payload) -> enqueue_command({'type':kind,'payload':payload})"""
    if not isinstance(payload, dict):
        raise ValueError("payload must be a dict")
    intent = {"type": str(kind), "payload": payload}
    return enqueue_command(intent, agent_id=agent_id, dedup_ttl_seconds=dedup_ttl_seconds, status=status)


def debug_dump_latest_commands(limit: int = 10) -> List[Tuple]:
    """Convenience helper for webshell debugging."""
    sql = """
        SELECT id, created_at, status, intent
        FROM commands
        ORDER BY id DESC
        LIMIT %s;
    """
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(limit),))
            rows = cur.fetchall()

    print("commands.columns = ['id','created_at','status','intent']")
    print("---- latest commands ----")
    for r in rows:
        print(r)
    return rows
