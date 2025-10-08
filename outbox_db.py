# outbox_db.py — SQLite storage for NovaTrade Command Bus (idempotent & lease-based)
import os, json, time, sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
LEASE_S = int(os.getenv("OUTBOX_LEASE_S", "120"))

def _conn():
    c = sqlite3.connect(DB_PATH, timeout=5)
    c.row_factory = sqlite3.Row
    return c

def _now_iso() -> str:
    return datetime.utcnow().isoformat()

def _ts_ms() -> int:
    return int(time.time() * 1000)

# ---------- DDL ----------
def init():
    with _conn() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS commands (
            id TEXT PRIMARY KEY,
            created_at TEXT,
            status TEXT,             -- PENDING | IN_FLIGHT | DONE | ERROR
            lease_expires_at TEXT,
            agent_target TEXT,       -- 'edge-x' or '*' (wildcard)
            type TEXT,               -- e.g., 'order.place'
            payload TEXT,            -- JSON
            hmac TEXT,               -- optional precomputed payload HMAC (not required)
            error TEXT,
            not_before INTEGER,      -- epoch ms
            dedupe_key TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS receipts (
            id TEXT PRIMARY KEY,
            cmd_id TEXT,
            ts TEXT,
            status TEXT,             -- ok | rejected | error | expired
            txid TEXT,
            fills TEXT,              -- JSON list
            message TEXT,
            agent_id TEXT,
            hmac TEXT
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cmd_status ON commands(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cmd_agent ON commands(agent_target)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_cmd_notbefore ON commands(not_before)")
    return True

# ---------- Helpers ----------
def _expire_stale_leases(c: sqlite3.Connection):
    now = _now_iso()
    c.execute(
        "UPDATE commands SET status='PENDING', lease_expires_at=NULL "
        "WHERE status='IN_FLIGHT' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
        (now,)
    )

def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] for k in row.keys()}

# ---------- API used by api_commands ----------
def enqueue(*, agent_id: str, kind: str, payload: Dict[str, Any],
            not_before: int = 0, dedupe_key: Optional[str] = None) -> str:
    """
    Insert a new command. Returns command id (string). If dedupe_key is provided and
    a PENDING/IN_FLIGHT command with same key exists, returns its id instead.
    """
    init()
    with _conn() as c:
        if dedupe_key:
            cur = c.execute(
                "SELECT id FROM commands WHERE dedupe_key=? AND status IN ('PENDING','IN_FLIGHT') LIMIT 1",
                (dedupe_key,)
            )
            hit = cur.fetchone()
            if hit:
                return hit["id"]

        cid = f"cmd_{_ts_ms()}"
        c.execute(
            """INSERT INTO commands
               (id, created_at, status, lease_expires_at, agent_target, type, payload, hmac, error, not_before, dedupe_key)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (cid, _now_iso(), "PENDING", None, agent_id or "*", kind,
             json.dumps(payload, separators=(",", ":"), sort_keys=True), None, None,
             int(not_before or 0), dedupe_key)
        )
        return cid

def pull_pending_for_agent(*, agent_id: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Return oldest PENDING commands for agent (or wildcard '*'), respecting not_before and expiring stale leases.
    """
    init()
    with _conn() as c:
        _expire_stale_leases(c)
        now_ms = _ts_ms()
        cur = c.execute(
            """SELECT id, type, payload, hmac
               FROM commands
               WHERE status='PENDING'
                 AND (agent_target=? OR agent_target='*' OR agent_target IS NULL)
                 AND (not_before IS NULL OR not_before=0 OR not_before <= ?)
               ORDER BY created_at ASC
               LIMIT ?""",
            (agent_id, now_ms, int(limit or 5))
        )
        rows = cur.fetchall()
        return [_row_to_dict(r) for r in rows]

def set_inflight_lease(cmd_id: str, ttl_s: int = None):
    """
    Mark command IN_FLIGHT and set lease expiry.
    """
    ttl = int(ttl_s or LEASE_S)
    lease_until = datetime.utcnow() + timedelta(seconds=ttl)
    with _conn() as c:
        c.execute(
            "UPDATE commands SET status='IN_FLIGHT', lease_expires_at=? WHERE id=?",
            (lease_until.isoformat(), cmd_id)
        )

def ack_receipt(*, cmd_id: str, agent_id: str, status: str, txid: Optional[str],
                fills: Any, message: Optional[str], ts: Optional[str], hmac: Optional[str]) -> str:
    """
    Store a receipt and finalize the command status.
    """
    rid = f"r_{_ts_ms()}"
    with _conn() as c:
        c.execute(
            """INSERT OR REPLACE INTO receipts
               (id, cmd_id, ts, status, txid, fills, message, agent_id, hmac)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (rid, cmd_id, ts, status, txid, json.dumps(fills or []), message, agent_id, hmac)
        )
        if status == "ok":
            c.execute("UPDATE commands SET status='DONE', error=NULL WHERE id=?", (cmd_id,))
        elif status in ("rejected", "error", "expired"):
            c.execute("UPDATE commands SET status='ERROR', error=? WHERE id=?", (message, cmd_id))
        else:
            c.execute("UPDATE commands SET status='ERROR', error=? WHERE id=?", (f"unknown status {status}", cmd_id))
    return rid
