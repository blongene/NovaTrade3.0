# outbox_db.py — Outbox adapter (AUTO: Postgres canonical commands table, fallback SQLite legacy)
# - Postgres mode uses public.commands:
#     id, created_at, agent_id, intent(jsonb), intent_hash(text unique), status, leased_by, lease_at, lease_expires_at, attempts, dedup_ttl_seconds
# - SQLite mode preserves legacy outbox.db schema used by older/local edge flows
#
# Env:
#   OUTBOX_MODE=auto|postgres|sqlite   (default auto)
#   DB_URL=postgres connection string  (required for postgres mode)
#   OUTBOX_DB_PATH=/data/outbox.db     (sqlite)
#   OUTBOX_LEASE_S=45                  (lease seconds)
#   OUTBOX_DEDUP_TTL_S=900             (postgres dedupe_ttl_seconds)
#
# API:
#   init()
#   enqueue(agent_id, kind_or_type, payload_or_intent, ..., dedupe_key=None, dedup_ttl_seconds=None) -> int
#   pull(agent_id, limit=10, lease_s=None, leased_by=None) -> list[dict]
#   ack(agent_id, receipts) -> None
#   reap_expired(...) -> int

from __future__ import annotations

import os
import json
import time
import sqlite3
import hashlib
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

OUTBOX_MODE = os.getenv("OUTBOX_MODE", "auto").strip().lower()
DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL")

DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
LEASE_S = int(os.getenv("OUTBOX_LEASE_S", "45"))
DEDUP_TTL_S = int(os.getenv("OUTBOX_DEDUP_TTL_S", "900"))

# ---------------------------
# Utilities
# ---------------------------

def _now_ts() -> int:
    return int(time.time())

def _json_dumps_stable(obj: Any) -> str:
    # Stable JSON for hashing
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _compute_intent_hash(intent: dict) -> str:
    return _sha256_hex(_json_dumps_stable(intent))

def _require_type(intent: dict) -> None:
    t = intent.get("type")
    if not isinstance(t, str) or not t.strip():
        raise ValueError("enqueue(): intent must include non-empty string field intent['type']")

def _detect_mode() -> str:
    if OUTBOX_MODE in ("postgres", "sqlite"):
        return OUTBOX_MODE
    # auto
    return "postgres" if DB_URL else "sqlite"

# ---------------------------
# Postgres implementation
# ---------------------------

def _pg_connect():
    import psycopg2  # type: ignore
    return psycopg2.connect(DB_URL)

@contextmanager
def _pg_conn():
    con = _pg_connect()
    try:
        yield con
    finally:
        con.close()

def _pg_init() -> None:
    # We do NOT create the canonical commands table here; migrations live elsewhere.
    # But we *do* sanity-check the connection.
    with _pg_conn() as con:
        with con.cursor() as cur:
            cur.execute("SELECT 1;")
        con.commit()

def _pg_enqueue(
    agent_id: str,
    intent: dict,
    *,
    status: str = "queued",
    dedup_ttl_seconds: Optional[int] = None,
    intent_hash: Optional[str] = None,
) -> int:
    _require_type(intent)
    ih = intent_hash or _compute_intent_hash(intent)
    ttl = int(dedup_ttl_seconds if dedup_ttl_seconds is not None else DEDUP_TTL_S)

    with _pg_conn() as con:
        with con.cursor() as cur:
            # On conflict (unique intent_hash), return existing id
            cur.execute(
                """
                INSERT INTO commands (created_at, agent_id, intent, intent_hash, status, attempts, dedup_ttl_seconds)
                VALUES (NOW(), %s, %s::jsonb, %s, %s, 0, %s)
                ON CONFLICT (intent_hash) DO UPDATE
                  SET dedup_ttl_seconds = EXCLUDED.dedup_ttl_seconds
                RETURNING id;
                """,
                (agent_id, json.dumps(intent), ih, status, ttl),
            )
            row = cur.fetchone()
        con.commit()
    return int(row[0]) if row else -1

def _pg_pull(
    agent_id: str,
    limit: int = 10,
    lease_s: Optional[int] = None,
    leased_by: Optional[str] = None,
) -> List[Dict[str, Any]]:
    lease_s = LEASE_S if lease_s is None else int(lease_s)
    leased_by = leased_by or agent_id
    # We lease commands that are queued and not currently leased, or whose lease expired.
    with _pg_conn() as con:
        with con.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute(
                """
                WITH cte AS (
                  SELECT id
                  FROM commands
                  WHERE status = 'queued'
                    AND agent_id = %s
                    AND (lease_expires_at IS NULL OR lease_expires_at <= NOW())
                  ORDER BY id ASC
                  LIMIT %s
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE commands c
                SET leased_by = %s,
                    lease_at = NOW(),
                    lease_expires_at = NOW() + (%s || ' seconds')::interval,
                    status = 'leased'
                FROM cte
                WHERE c.id = cte.id
                RETURNING c.id, c.created_at, c.agent_id, c.status, c.intent, c.intent_hash;
                """,
                (agent_id, int(limit), leased_by, int(lease_s)),
            )
            rows = cur.fetchall()
        con.commit()

    out: List[Dict[str, Any]] = []
    for (cid, created_at, agent, status, intent, intent_hash) in rows:
        out.append(
            {
                "id": int(cid),
                "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
                "agent_id": agent,
                "status": status,
                "intent": intent,
                "intent_hash": intent_hash,
            }
        )
    return out

def _pg_ack(agent_id: str, receipts: List[Dict[str, Any]]) -> None:
    """
    receipts: [{id:<cmd_id>, ok:bool, status:str?, txid:str?, message:str?, result:dict?}]
    Note: canonical receipts table is handled elsewhere in your stack; this function only marks commands.
    """
    with _pg_conn() as con:
        with con.cursor() as cur:
            for r in receipts:
                cmd_id = int(r["id"])
                ok = bool(r.get("ok"))
                new_status = "done" if ok else "error"
                cur.execute(
                    """
                    UPDATE commands
                    SET status = %s,
                        leased_by = NULL,
                        lease_at = NULL,
                        lease_expires_at = NULL,
                        attempts = CASE WHEN %s THEN attempts ELSE attempts + 1 END
                    WHERE id = %s AND agent_id = %s;
                    """,
                    (new_status, ok, cmd_id, agent_id),
                )
        con.commit()

def _pg_reap_expired(agent_id: Optional[str] = None) -> int:
    with _pg_conn() as con:
        with con.cursor() as cur:
            if agent_id:
                cur.execute(
                    """
                    UPDATE commands
                    SET status='queued', leased_by=NULL, lease_at=NULL, lease_expires_at=NULL
                    WHERE agent_id=%s AND status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= NOW();
                    """,
                    (agent_id,),
                )
            else:
                cur.execute(
                    """
                    UPDATE commands
                    SET status='queued', leased_by=NULL, lease_at=NULL, lease_expires_at=NULL
                    WHERE status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= NOW();
                    """
                )
            n = cur.rowcount or 0
        con.commit()
    return int(n)

# ---------------------------
# SQLite legacy implementation
# ---------------------------

DDL_SQLITE = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS commands (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL,
  not_before INTEGER DEFAULT 0,
  dedupe_key TEXT DEFAULT NULL,
  status TEXT NOT NULL DEFAULT 'pending',    -- pending|in_flight|done|error
  lease_expires_at INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_cmd_agent_status ON commands(agent_id, status);
CREATE INDEX IF NOT EXISTS ix_cmd_dedupe ON commands(agent_id, dedupe_key);

CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cmd_id INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  ok INTEGER NOT NULL,
  status TEXT DEFAULT NULL,
  txid TEXT DEFAULT NULL,
  message TEXT DEFAULT NULL,
  received_at INTEGER NOT NULL,
  result TEXT NOT NULL,
  FOREIGN KEY(cmd_id) REFERENCES commands(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_receipts_cmd_agent ON receipts(cmd_id, agent_id);
"""

@contextmanager
def _sq_conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()

def _sq_col_exists(con, table, col) -> bool:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)

def _sq_init() -> None:
    with _sq_conn() as con:
        con.executescript(DDL_SQLITE)
        # migrations if running against older schema
        if not _sq_col_exists(con, "commands", "lease_expires_at"):
            con.execute("ALTER TABLE commands ADD COLUMN lease_expires_at INTEGER DEFAULT 0")
        if not _sq_col_exists(con, "commands", "status"):
            con.execute("ALTER TABLE commands ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'")
        # receipts optional columns
        if not _sq_col_exists(con, "receipts", "status"):
            con.execute("ALTER TABLE receipts ADD COLUMN status TEXT DEFAULT NULL")
        if not _sq_col_exists(con, "receipts", "txid"):
            con.execute("ALTER TABLE receipts ADD COLUMN txid TEXT DEFAULT NULL")
        if not _sq_col_exists(con, "receipts", "message"):
            con.execute("ALTER TABLE receipts ADD COLUMN message TEXT DEFAULT NULL")
        con.commit()

def _sq_enqueue(agent_id: str, kind: str, payload: dict, not_before: int = 0, dedupe_key: Optional[str] = None) -> int:
    with _sq_conn() as con:
        if dedupe_key:
            cur = con.execute(
                "SELECT id FROM commands WHERE agent_id=? AND status IN ('pending','in_flight') AND dedupe_key=?",
                (agent_id, dedupe_key),
            )
            if cur.fetchone():
                return -1
        con.execute(
            "INSERT INTO commands(created_at,agent_id,kind,payload,not_before,dedupe_key,status,lease_expires_at)"
            " VALUES(?,?,?,?,?,?, 'pending', 0)",
            (_now_ts(), agent_id, kind, json.dumps(payload), int(not_before or 0), dedupe_key),
        )
        con.commit()
        return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])

def _sq_pull(agent_id: str, limit: int = 10, lease_s: Optional[int] = None):
    lease_s = LEASE_S if lease_s is None else int(lease_s)
    now = _now_ts()
    with _sq_conn() as con:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute(
            """
            SELECT id FROM commands
            WHERE agent_id=? AND status='pending'
              AND (not_before=0 OR not_before<=?)
            ORDER BY id ASC LIMIT ?;
            """,
            (agent_id, now, int(limit)),
        ).fetchall()

        ids = [r["id"] for r in rows]
        if not ids:
            con.commit()
            return []

        exp = now + lease_s
        qmarks = ",".join("?" * len(ids))
        con.execute(
            f"UPDATE commands SET status='in_flight', lease_expires_at=? WHERE id IN ({qmarks}) AND status='pending'",
            (exp, *ids),
        )
        rows2 = con.execute(
            f"SELECT id,created_at,kind,payload FROM commands WHERE id IN ({qmarks})",
            (*ids,),
        ).fetchall()
        con.commit()

    return [
        dict(id=int(r["id"]), created_at=int(r["created_at"]), kind=r["kind"], payload=json.loads(r["payload"]))
        for r in rows2
    ]

def _sq_ack(agent_id: str, receipts: List[Dict[str, Any]]) -> None:
    now = _now_ts()
    with _sq_conn() as con:
        for r in receipts:
            cmd_id = int(r["id"])
            ok = 1 if r.get("ok") else 0
            status = r.get("status")
            txid = r.get("txid")
            msg = r.get("message")
            result = json.dumps(r.get("result", {}))
            con.execute(
                "INSERT OR IGNORE INTO receipts(cmd_id,agent_id,ok,status,txid,message,received_at,result)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (cmd_id, agent_id, ok, status, txid, msg, now, result),
            )
            new_status = "done" if ok else "error"
            con.execute(
                "UPDATE commands SET status=?, lease_expires_at=0 WHERE id=? AND agent_id=?",
                (new_status, cmd_id, agent_id),
            )
        con.commit()

def _sq_reap_expired(now: Optional[int] = None) -> int:
    now = _now_ts() if now is None else int(now)
    with _sq_conn() as con:
        cur = con.execute(
            "UPDATE commands SET status='pending', lease_expires_at=0 "
            "WHERE status='in_flight' AND lease_expires_at>0 AND lease_expires_at<=?",
            (now,),
        )
        n = cur.rowcount or 0
        con.commit()
        return int(n)

# ---------------------------
# Public API (mode-switching)
# ---------------------------

def init() -> None:
    mode = _detect_mode()
    if mode == "postgres":
        _pg_init()
    else:
        _sq_init()

def enqueue(
    agent_id: str,
    kind_or_type: str,
    payload_or_intent: dict,
    not_before: int = 0,
    dedupe_key: Optional[str] = None,
    *,
    dedup_ttl_seconds: Optional[int] = None,
    intent_hash: Optional[str] = None,
) -> int:
    """
    - Postgres: kind_or_type is ignored; payload_or_intent must be an *intent dict* with intent['type'] set.
    - SQLite: kind_or_type is stored as 'kind', payload_or_intent stored as 'payload'.
    """
    mode = _detect_mode()
    if mode == "postgres":
        intent = payload_or_intent
        return _pg_enqueue(
            agent_id=agent_id,
            intent=intent,
            status="queued",
            dedup_ttl_seconds=dedup_ttl_seconds,
            intent_hash=intent_hash,
        )
    else:
        return _sq_enqueue(agent_id, kind_or_type, payload_or_intent, not_before=not_before, dedupe_key=dedupe_key)

def pull(agent_id: str, limit: int = 10, lease_s: Optional[int] = None, leased_by: Optional[str] = None):
    mode = _detect_mode()
    if mode == "postgres":
        return _pg_pull(agent_id=agent_id, limit=limit, lease_s=lease_s, leased_by=leased_by)
    else:
        return _sq_pull(agent_id=agent_id, limit=limit, lease_s=lease_s)

def ack(agent_id: str, receipts: List[Dict[str, Any]]) -> None:
    mode = _detect_mode()
    if mode == "postgres":
        _pg_ack(agent_id=agent_id, receipts=receipts)
    else:
        _sq_ack(agent_id=agent_id, receipts=receipts)

def reap_expired(*, agent_id: Optional[str] = None, now: Optional[int] = None) -> int:
    mode = _detect_mode()
    if mode == "postgres":
        return _pg_reap_expired(agent_id=agent_id)
    else:
        return _sq_reap_expired(now=now)
