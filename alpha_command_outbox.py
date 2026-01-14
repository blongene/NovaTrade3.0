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
    intent: Dict[str, Any],
    *,
    agent_id: str = "cloud",
    status: str = "queued",
    dedup_ttl_seconds: int = 900,
) -> EnqueueResult:
    """
    Insert an intent into commands table with idempotency on intent_hash.

    - If new row inserted: returns created_new=True
    - If intent_hash already exists: returns existing id and created_new=False
    """
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
                # Extremely unlikely, but make it explicit
                raise RuntimeError("ON CONFLICT DO NOTHING but could not re-select existing command id")
            return EnqueueResult(cmd_id=int(row2[0]), intent_hash=ih, created_new=False)


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
