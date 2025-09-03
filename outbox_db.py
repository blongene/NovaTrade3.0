# outbox_db.py — SQLite outbox with leases (pending|in_flight|done|error)
import os, sqlite3, json, time
from contextlib import contextmanager

DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
LEASE_S = int(os.getenv("OUTBOX_LEASE_S", "45"))

DDL = """
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

def _get_conn():
    return sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)

@contextmanager
def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    yield con
    con.close()

def _col_exists(con, table, col):
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)

def init():
    with _conn() as con:
        con.executescript(DDL)
        # migrations if running against older schema
        if not _col_exists(con, "commands", "lease_expires_at"):
            con.execute("ALTER TABLE commands ADD COLUMN lease_expires_at INTEGER DEFAULT 0")
        if not _col_exists(con, "commands", "status"):
            con.execute("ALTER TABLE commands ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'")
        # receipts optional columns
        if not _col_exists(con, "receipts", "status"):
            con.execute("ALTER TABLE receipts ADD COLUMN status TEXT DEFAULT NULL")
        if not _col_exists(con, "receipts", "txid"):
            con.execute("ALTER TABLE receipts ADD COLUMN txid TEXT DEFAULT NULL")
        if not _col_exists(con, "receipts", "message"):
            con.execute("ALTER TABLE receipts ADD COLUMN message TEXT DEFAULT NULL")
        con.commit()

def enqueue(agent_id: str, kind: str, payload: dict, not_before: int = 0, dedupe_key: str | None = None) -> int:
    with _conn() as con:
        if dedupe_key:
            cur = con.execute(
                "SELECT id FROM commands WHERE agent_id=? AND status IN ('pending','in_flight') AND dedupe_key=?",
                (agent_id, dedupe_key)
            )
            if cur.fetchone():
                return -1
        con.execute(
            "INSERT INTO commands(created_at,agent_id,kind,payload,not_before,dedupe_key,status,lease_expires_at)"
            " VALUES(?,?,?,?,?,?, 'pending', 0)",
            (int(time.time()), agent_id, kind, json.dumps(payload), not_before, dedupe_key)
        )
        con.commit()
        return con.execute("SELECT last_insert_rowid()").fetchone()[0]

def pull(agent_id: str, limit: int = 10, lease_s: int = None):
    lease_s = LEASE_S if lease_s is None else int(lease_s)
    now = int(time.time())
    with _conn() as con:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute("""
            SELECT id FROM commands
            WHERE agent_id=? AND status='pending'
              AND (not_before=0 OR not_before<=?)
            ORDER BY id ASC LIMIT ?""", (agent_id, now, limit)).fetchall()
        ids = [r["id"] for r in rows]
        if not ids:
            con.commit()
            return []
        exp = now + lease_s
        qmarks = ",".join("?"*len(ids))
        con.execute(f"UPDATE commands SET status='in_flight', lease_expires_at=? WHERE id IN ({qmarks}) AND status='pending'", (exp, *ids))
        rows2 = con.execute(f"SELECT id,created_at,kind,payload FROM commands WHERE id IN ({qmarks})", (*ids,)).fetchall()
        con.commit()
    return [dict(id=r["id"], created_at=r["created_at"], kind=r["kind"], payload=json.loads(r["payload"])) for r in rows2]

def ack(agent_id: str, receipts: list[dict]):
    """
    receipts: [{id:<cmd_id>, ok:bool, status:str?, txid:str?, message:str?, result:dict}]
    """
    now = int(time.time())
    with _conn() as con:
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
                (cmd_id, agent_id, ok, status, txid, msg, now, result)
            )
            new_status = "done" if ok else "error"
            con.execute(
                "UPDATE commands SET status=?, lease_expires_at=0 WHERE id=? AND agent_id=?",
                (new_status, cmd_id, agent_id)
            )
        con.commit()

def reap_expired(now: int | None = None):
    """Optional helper for a cron: return expired in_flight commands to pending."""
    now = int(time.time()) if now is None else int(now)
    with _conn() as con:
        cur = con.execute(
            "UPDATE commands SET status='pending', lease_expires_at=0 "
            "WHERE status='in_flight' AND lease_expires_at>0 AND lease_expires_at<=?",
            (now,)
        )
        n = cur.rowcount or 0
        con.commit()
        return n
