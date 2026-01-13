# alpha_command_outbox.py
from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, Optional, Tuple

from db import get_db_conn  # keep your existing db helper import if different


DEFAULT_AGENT_ID = os.getenv("CLOUD_AGENT_ID", "cloud")
DEFAULT_DEDUP_TTL_SECONDS = int(os.getenv("COMMANDS_DEDUP_TTL_SECONDS", "900"))


def _stable_json(obj: Any) -> str:
    """Stable JSON string for hashing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def ensure_intent_has_type(intent: Any) -> Dict[str, Any]:
    """
    Guarantees intent is a dict and has a top-level 'type' key.

    - {} becomes: {'type':'invalid','note':'auto-fixed empty intent'}
    - dict without 'type' becomes: {'type':'legacy.command','legacy': <original>}
    - non-dict becomes: {'type':'legacy.command','legacy': {'value': <original>}}
    """
    if intent is None:
        return {"type": "invalid", "note": "auto-fixed null intent"}
    if not isinstance(intent, dict):
        return {"type": "legacy.command", "legacy": {"value": intent}}

    if intent == {}:
        return {"type": "invalid", "note": "auto-fixed empty intent"}

    if "type" in intent and isinstance(intent["type"], str) and intent["type"].strip():
        return intent

    # If it looks like an older shape, wrap it so the constraint is satisfied
    return {"type": "legacy.command", "legacy": intent}


def compute_intent_hash(intent: Dict[str, Any]) -> str:
    intent = ensure_intent_has_type(intent)
    return _sha256_hex(_stable_json(intent))


def enqueue_command(
    intent: Dict[str, Any],
    *,
    agent_id: Optional[str] = None,
    status: str = "queued",
    dedup_ttl_seconds: Optional[int] = None,
) -> Tuple[int, str]:
    """
    Inserts into canonical commands table:
      (agent_id, intent, intent_hash, status, dedup_ttl_seconds)

    Returns: (cmd_id, intent_hash)
    """
    agent_id = agent_id or DEFAULT_AGENT_ID
    dedup_ttl_seconds = int(dedup_ttl_seconds or DEFAULT_DEDUP_TTL_SECONDS)

    intent = ensure_intent_has_type(intent)
    intent_hash = compute_intent_hash(intent)

    sql = """
        INSERT INTO commands (agent_id, intent, intent_hash, status, dedup_ttl_seconds)
        VALUES (%s, %s::jsonb, %s, %s, %s)
        ON CONFLICT (intent_hash) DO NOTHING
        RETURNING id;
    """

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (agent_id, json.dumps(intent), intent_hash, status, dedup_ttl_seconds))
            row = cur.fetchone()
            conn.commit()

    # If dedup prevented insert, we still want to return the existing id
    if not row:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM commands WHERE intent_hash=%s;", (intent_hash,))
                got = cur.fetchone()
                if not got:
                    raise RuntimeError("Dedup hit but could not find existing command by intent_hash.")
                return int(got[0]), intent_hash

    return int(row[0]), intent_hash


def debug_dump_latest_commands(limit: int = 10) -> None:
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
    print("---- latest commands ----")
    for r in rows:
        print(r)
