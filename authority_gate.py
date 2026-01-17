#!/usr/bin/env python3
"""
NovaTrade Bus — Authority Gate (DB-first) — Phase 28.2 Step 4

Used by wsgi.py:
  from authority_gate import evaluate_agent, lease_block_response

DB schema (auto-created):
  agent_authority(
    agent_id   text primary key,
    trusted    boolean not null default false,
    reason     text,
    last_seen  timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
  )

Env:
  AUTHORITY_GATE_ENABLED         default "1"  (1/true enables enforcement)
  AUTHORITY_FAIL_OPEN            default "0"  (1 = allow if DB errors; 0 = block if DB errors)
  AUTHORITY_BOOTSTRAP_TRUSTED    default ""   (comma-separated agent_ids to trust on boot)
  AUTHORITY_DEFAULT_TRUSTED      default "0"  (if agent missing from table, trust it? usually 0)
"""

from __future__ import annotations

import os
import threading
from typing import Optional, Tuple, Dict, Any

# Prefer DB_URL, but also accept DATABASE_URL
_DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL") or ""

AUTHORITY_GATE_ENABLED = os.getenv("AUTHORITY_GATE_ENABLED", "1").lower() in ("1", "true", "yes", "on")
AUTHORITY_FAIL_OPEN = os.getenv("AUTHORITY_FAIL_OPEN", "0").lower() in ("1", "true", "yes", "on")
AUTHORITY_DEFAULT_TRUSTED = os.getenv("AUTHORITY_DEFAULT_TRUSTED", "0").lower() in ("1", "true", "yes", "on")

_BOOTSTRAP_TRUSTED = [
    a.strip() for a in (os.getenv("AUTHORITY_BOOTSTRAP_TRUSTED", "") or "").split(",") if a.strip()
]

_init_lock = threading.Lock()
_inited = False

# Optional utils hooks (logging + telegram)
try:
    from utils import info, warn, send_telegram_message_dedup  # type: ignore
except Exception:
    def info(msg: str) -> None:  # type: ignore
        print("[INFO]", msg)

    def warn(msg: str) -> None:  # type: ignore
        print("[WARN]", msg)

    def send_telegram_message_dedup(message: str, key: str, ttl_min: int = 15) -> None:  # type: ignore
        # Silent fallback
        return


def _connect():
    """
    Create a short-lived DB connection.

    Uses psycopg2 if available (most common on Render).
    """
    if not _DB_URL:
        raise RuntimeError("DB_URL/DATABASE_URL missing")
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(_DB_URL, connect_timeout=5)
    except Exception as e:
        raise RuntimeError(f"psycopg2 connect failed: {e}") from e


def _ensure_schema() -> None:
    global _inited
    if _inited:
        return
    with _init_lock:
        if _inited:
            return

        if not _DB_URL:
            warn("authority_gate: DB_URL missing; authority gate cannot use DB.")
            _inited = True
            return

        try:
            conn = _connect()
            try:
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(
                    """
                    create table if not exists agent_authority (
                      agent_id   text primary key,
                      trusted    boolean not null default false,
                      reason     text,
                      last_seen  timestamptz not null default now(),
                      created_at timestamptz not null default now(),
                      updated_at timestamptz not null default now()
                    );
                    """
                )
                cur.execute(
                    "create index if not exists idx_agent_authority_last_seen on agent_authority (last_seen desc);"
                )
                cur.close()
            finally:
                conn.close()

            # Bootstrap trust list (idempotent)
            if _BOOTSTRAP_TRUSTED:
                for agent_id in _BOOTSTRAP_TRUSTED:
                    try:
                        set_agent_trust(agent_id, True, reason="bootstrap trusted (AUTHORITY_BOOTSTRAP_TRUSTED)")
                    except Exception as e:
                        warn(f"authority_gate: bootstrap trust failed for {agent_id}: {e!r}")

            info("authority_gate: schema ready (agent_authority)")
        except Exception as e:
            warn(f"authority_gate: schema init failed: {e!r}")
        finally:
            _inited = True


def set_agent_trust(agent_id: str, trusted: bool, reason: str = "") -> None:
    """
    Upsert an agent trust decision.
    """
    _ensure_schema()
    if not _DB_URL:
        raise RuntimeError("DB_URL missing")

    agent_id = (agent_id or "").strip()
    if not agent_id:
        raise ValueError("agent_id required")

    conn = _connect()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            insert into agent_authority (agent_id, trusted, reason, last_seen, updated_at)
            values (%s, %s, %s, now(), now())
            on conflict (agent_id) do update set
              trusted = excluded.trusted,
              reason = excluded.reason,
              last_seen = excluded.last_seen,
              updated_at = excluded.updated_at;
            """,
            (agent_id, bool(trusted), reason or None),
        )
        cur.close()
    finally:
        conn.close()


def _touch_agent(agent_id: str) -> None:
    """
    Ensure the agent exists in the table and update last_seen.
    Does NOT change trusted value if it already exists.
    """
    _ensure_schema()
    if not _DB_URL:
        return

    agent_id = (agent_id or "").strip()
    if not agent_id:
        return

    conn = _connect()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            insert into agent_authority (agent_id, trusted, reason, last_seen, updated_at)
            values (%s, %s, %s, now(), now())
            on conflict (agent_id) do update set
              last_seen = excluded.last_seen,
              updated_at = excluded.updated_at;
            """,
            (agent_id, bool(AUTHORITY_DEFAULT_TRUSTED), "auto-seen" if not AUTHORITY_DEFAULT_TRUSTED else "auto-trusted"),
        )
        cur.close()
    finally:
        conn.close()


def evaluate_agent(agent_id: str) -> Tuple[bool, str, int]:
    """
    Returns: (trusted, reason, age_sec)

    - If AUTHORITY_GATE_ENABLED is False -> always trusted=True
    - Updates last_seen on every call (when DB is available)
    - Reads trust decision from agent_authority
    """
    _ensure_schema()

    agent_id = (agent_id or "").strip() or "edge"

    # If gate disabled, behave permissively (but still try to touch for observability)
    if not AUTHORITY_GATE_ENABLED:
        try:
            _touch_agent(agent_id)
        except Exception:
            pass
        return True, "authority_gate_disabled", 0

    # Gate enabled but no DB: choose fail-open vs fail-closed
    if not _DB_URL:
        if AUTHORITY_FAIL_OPEN:
            warn("authority_gate: DB_URL missing; FAIL_OPEN allowing agent")
            return True, "authority_db_missing_fail_open", 0
        return False, "authority_db_missing_fail_closed", 0

    # Touch first (records contact even if untrusted)
    try:
        _touch_agent(agent_id)
    except Exception as e:
        if AUTHORITY_FAIL_OPEN:
            warn(f"authority_gate: touch failed; FAIL_OPEN allowing agent={agent_id}: {e!r}")
            return True, "authority_touch_failed_fail_open", 0
        warn(f"authority_gate: touch failed; FAIL_CLOSED blocking agent={agent_id}: {e!r}")
        return False, "authority_touch_failed_fail_closed", 0

    # Read trust + age
    try:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                select
                  trusted,
                  coalesce(reason, ''),
                  extract(epoch from (now() - last_seen))::int as age_sec
                from agent_authority
                where agent_id = %s
                limit 1;
                """,
                (agent_id,),
            )
            row = cur.fetchone()
            cur.close()
        finally:
            conn.close()

        if not row:
            # Should not happen because _touch_agent inserts, but keep defensive
            trusted = bool(AUTHORITY_DEFAULT_TRUSTED)
            reason = "missing_row_default_trusted" if trusted else "missing_row_default_untrusted"
            age_sec = 0
            return trusted, reason, age_sec

        trusted = bool(row[0])
        reason = str(row[1] or "")
        age_sec = int(row[2] or 0)

        if trusted:
            return True, reason or "trusted", age_sec

        # Untrusted: notify (de-duped)
        try:
            send_telegram_message_dedup(
                f"🚫 *Authority Gate*: blocked agent `{agent_id}` (untrusted).",
                key=f"auth_block:{agent_id}",
                ttl_min=60,
            )
        except Exception:
            pass

        return False, reason or "untrusted", age_sec

    except Exception as e:
        if AUTHORITY_FAIL_OPEN:
            warn(f"authority_gate: read failed; FAIL_OPEN allowing agent={agent_id}: {e!r}")
            return True, "authority_read_failed_fail_open", 0
        warn(f"authority_gate: read failed; FAIL_CLOSED blocking agent={agent_id}: {e!r}")
        return False, "authority_read_failed_fail_closed", 0


def lease_block_response(agent_id: str) -> Dict[str, Any]:
    """
    Return a soft-block response (200 OK) shaped like /api/commands/pull expects.
    wsgi.py can add lease_seconds before returning.
    """
    agent_id = (agent_id or "").strip() or "edge"
    trusted, reason, age = evaluate_agent(agent_id)
    return {
        "ok": True,
        "commands": [],
        "hold": True,
        "trusted": bool(trusted),
        "reason": reason,
        "agent_id": agent_id,
        "age_sec": int(age or 0),
    }
