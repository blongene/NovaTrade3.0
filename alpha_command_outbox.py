# alpha_command_outbox.py
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import psycopg2
import psycopg2.extras


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _db_url() -> str:
    url = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DB_URL (or DATABASE_URL) is not set")
    return url


def get_db_conn():
    """
    Postgres connector for NovaTrade commands outbox.
    Self-contained on purpose: DO NOT depend on db.py existing.
    """
    return psycopg2.connect(_db_url())


def _canonical_json(obj: Any) -> str:
    """
    Deterministic JSON string for hashing + dedupe.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_intent_hash(intent: Dict[str, Any]) -> str:
    """
    Stable hash used for unique constraint commands(intent_hash).
    """
    s = _canonical_json(intent)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _ensure_type(intent: Dict[str, Any]) -> None:
    """
    Enforce the DB constraint: commands.intent must have 'type'.
    """
    t = (intent or {}).get("type")
    if not t or not isinstance(t, str):
        raise ValueError("intent must include a non-empty string field: intent['type']")


@dataclass
class EnqueueResult:
    cmd_id: int
    intent_hash: str
    created_new: bool


def enqueue_command(
    *,
    agent_id: str,
    intent: dict | None = None,
    kind: str | None = None,
    payload: dict | None = None,
    dedup_ttl_seconds: int = 900,
):
    """
    Backward/forward compatible enqueue.

    Accepts either:
      - enqueue_command(agent_id=..., intent={...})
      - enqueue_command(agent_id=..., kind="order.place", payload={...})

    Returns whatever your module already returns (often EnqueueResult).
    """

    if intent is None:
        if not kind:
            raise TypeError("enqueue_command requires either intent=... or kind=...")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")

        intent = {"type": str(kind), "payload": payload}

    if not isinstance(intent, dict):
        raise TypeError("intent must be a dict")

    # If your existing implementation already has an internal enqueue() or insert,
    # keep using it. The only goal here is to accept kind/payload without breaking.
    #
    # Common existing call patterns in your repo:
    #   return enqueue(agent_id=agent_id, intent=intent, dedup_ttl_seconds=dedup_ttl_seconds)
    # OR
    #   return _enqueue_impl(...)
    #
    # Adjust the next line to match your file’s internal function name if needed.
    return enqueue(agent_id=agent_id, intent=intent, dedup_ttl_seconds=dedup_ttl_seconds)

def debug_dump_latest_commands(limit: int = 10) -> List[Tuple]:
    """
    Convenience helper for webshell debugging.
    """
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
