# bus_store.py
# Durable outbox + leases + receipts for NovaTrade Bus (Phase B)
import os, sqlite3, time, json, uuid
from typing import List, Dict, Optional, Tuple

DB_PATH = os.getenv("OUTBOX_DB_PATH", "./outbox.sqlite")

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  payload TEXT NOT NULL,               -- JSON string (intent)
  status TEXT NOT NULL DEFAULT 'queued', -- queued|leased|acked|failed
  leased_by TEXT,                      -- agent_id holding lease
  lease_until INTEGER,                 -- epoch seconds
  idempotency_key TEXT,                -- optional dedupe key
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS receipts (
  id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  agent_id TEXT,
  status TEXT NOT NULL,                -- ok|error|skipped
  payload TEXT,                        -- JSON string (receipt details)
  created_at INTEGER NOT NULL,
  UNIQUE(command_id)                   -- 1 receipt per command (idempotent)
);

CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status);
CREATE INDEX IF NOT EXISTS idx_commands_lease ON commands(lease_until);
CREATE INDEX IF NOT EXISTS idx_commands_idem ON commands(idempotency_key);
"""

def _now() -> int:
    return int(time.time())

def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def ensure_db():
    with _conn() as c:
        c.executescript(SCHEMA)

def enqueue_command(payload: Dict, idempotency_key: Optional[str] = None) -> str:
    """
    Store a new command. If idempotency_key matches an existing queued/leased command,
    return its id instead of inserting a duplicate.
    """
    ensure_db()
    with _conn() as c:
        if idempotency_key:
            row = c.execute(
                "SELECT id FROM commands WHERE idempotency_key = ? AND status IN ('queued','leased')",
                (idempotency_key,)
            ).fetchone()
            if row:
                return row[0]
        cmd_id = payload.get("id") or str(uuid.uuid4())
        now = _now()
        c.execute(
            "INSERT OR REPLACE INTO commands (id, payload, status, leased_by, lease_until, idempotency_key, created_at, updated_at) "
            "VALUES (?, ?, COALESCE((SELECT status FROM commands WHERE id=?), 'queued'), "
            "COALESCE((SELECT leased_by FROM commands WHERE id=?), NULL), "
            "COALESCE((SELECT lease_until FROM commands WHERE id=?), NULL), ?, "
            "COALESCE((SELECT created_at FROM commands WHERE id=?), ?), ?)",
            (cmd_id, json.dumps(payload, separators=(",",":")), cmd_id, cmd_id, cmd_id,
             idempotency_key, cmd_id, now, now)
        )
        return cmd_id

def pull_commands(agent_id: str, max_items: int = 10, lease_ttl_sec: int = 90) -> List[Dict]:
    """
    Lease up to N available commands to agent_id.
    Eligible: status='queued' OR (status='leased' AND lease expired).
    """
    ensure_db()
    now = _now()
    lease_until = now + lease_ttl_sec
    out: List[Dict] = []
    with _conn() as c:
        # Select candidates
        rows = c.execute(
            """
            SELECT id, payload FROM commands
            WHERE
              (status='queued')
              OR (status='leased' AND (lease_until IS NULL OR lease_until < ?))
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now, max_items)
        ).fetchall()

        for rid, payload in rows:
            # Lease them atomically
            updated = c.execute(
                """
                UPDATE commands
                SET status='leased', leased_by=?, lease_until=?, updated_at=?
                WHERE id=? AND (
                    status='queued'
                    OR (status='leased' AND (lease_until IS NULL OR lease_until < ?))
                )
                """,
                (agent_id, lease_until, now, rid, now)
            )
            if updated.rowcount > 0:
                out.append(json.loads(payload))
        return out

def renew_leases(agent_id: str, command_ids: List[str], lease_ttl_sec: int = 90) -> int:
    """Optional: renew leases if still held by agent_id."""
    ensure_db()
    now = _now()
    lease_until = now + lease_ttl_sec
    with _conn() as c:
        qmarks = ",".join(["?"] * len(command_ids))
        params = [lease_until, now, agent_id] + command_ids
        res = c.execute(
            f"UPDATE commands SET lease_until=?, updated_at=? "
            f"WHERE leased_by=? AND id IN ({qmarks})",
            params
        )
        return res.rowcount

def ack_command(agent_id: str, command_id: str, status: str, receipt_payload: Dict) -> bool:
    """
    Mark a command as acked/failed and write a receipt (idempotent).
    Only the leasing agent can ACK (or anyone if command is not leased).
    """
    ensure_db()
    now = _now()
    with _conn() as c:
        row = c.execute(
            "SELECT status, leased_by FROM commands WHERE id=?",
            (command_id,)
        ).fetchone()
        if not row:
            # Unknown command → accept idempotently by writing a receipt anyway
            try:
                c.execute(
                    "INSERT OR IGNORE INTO receipts (id, command_id, agent_id, status, payload, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (str(uuid.uuid4()), command_id, agent_id, status, json.dumps(receipt_payload, separators=(",",":")), now)
                )
            except Exception:
                pass
            return True

        cur_status, leased_by = row
        # If leased, enforce same agent
        if cur_status == "leased" and leased_by and leased_by != agent_id:
            return False

        # Transition + receipt
        c.execute(
            "UPDATE commands SET status=?, leased_by=NULL, lease_until=NULL, updated_at=? WHERE id=?",
            ("acked" if status == "ok" else "failed", now, command_id)
        )
        c.execute(
            "INSERT OR REPLACE INTO receipts (id, command_id, agent_id, status, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), command_id, agent_id, status, json.dumps(receipt_payload, separators=(",",":")), now)
        )
        return True

def queue_depth() -> Dict[str, int]:
    ensure_db()
    with _conn() as c:
        out: Dict[str,int] = {}
        for s in ("queued","leased","acked","failed"):
            out[s] = c.execute("SELECT COUNT(*) FROM commands WHERE status=?", (s,)).fetchone()[0]
        return out

def last_receipts(n: int = 10) -> List[Dict]:
    ensure_db()
    with _conn() as c:
        rows = c.execute(
            "SELECT command_id, agent_id, status, payload, created_at FROM receipts ORDER BY created_at DESC LIMIT ?",
            (n,)
        ).fetchall()
        return [
            {
                "command_id": r[0],
                "agent_id": r[1],
                "status": r[2],
                "payload": json.loads(r[3]) if r[3] else None,
                "ts": r[4],
            } for r in rows
        ]
