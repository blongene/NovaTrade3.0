# outbox_db.py — durable SQLite outbox with leases & idempotent receipts (cloud)
import os, sqlite3, json, time
from contextlib import contextmanager

DB_PATH = os.getenv("OUTBOX_DB_PATH", "./data/outbox.db")

# ensure directory exists (works for relative or absolute paths)
_dir = os.path.dirname(os.path.abspath(DB_PATH)) or "."
os.makedirs(_dir, exist_ok=True)

BUSY_TIMEOUT_MS = int(os.getenv("OUTBOX_SQLITE_BUSY_MS", "5000"))  # 5s
DEFAULT_LEASE_S = int(os.getenv("OUTBOX_DEFAULT_LEASE_S", "180"))  # 3m

DDL = """
CREATE TABLE IF NOT EXISTS commands (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at       INTEGER NOT NULL,
  agent_id         TEXT    NOT NULL,
  kind             TEXT    NOT NULL,
  payload          TEXT    NOT NULL,
  not_before       INTEGER DEFAULT 0,
  dedupe_key       TEXT,
  status           TEXT    NOT NULL DEFAULT 'pending',  -- pending|in_flight|done|error|expired
  lease_expires_at INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_cmd_agent_status ON commands(agent_id, status);
CREATE INDEX IF NOT EXISTS ix_cmd_dedupe        ON commands(agent_id, dedupe_key);

CREATE TABLE IF NOT EXISTS receipts (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  cmd_id      INTEGER NOT NULL,
  agent_id    TEXT    NOT NULL,
  ok          INTEGER NOT NULL,
  status      TEXT,                 -- ok|error|rejected|expired (optional semantic)
  received_at INTEGER NOT NULL,
  txid        TEXT,
  fills       TEXT,                 -- JSON list
  message     TEXT,
  result      TEXT NOT NULL,        -- JSON blob (always present)
  UNIQUE(cmd_id, agent_id),
  FOREIGN KEY(cmd_id) REFERENCES commands(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_receipts_cmd_agent ON receipts(cmd_id, agent_id);
"""

def _connect():
    con = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout = %d;" % BUSY_TIMEOUT_MS)
    # Optional durability tuning:
    con.execute("PRAGMA synchronous=NORMAL;")
    return con

@contextmanager
def _conn():
    con = _connect()
    try:
        yield con
    finally:
        con.close()

def _ensure_columns():
    """Add columns if upgrading from older schema."""
    with _conn() as con:
        cols = {r["name"] for r in con.execute("PRAGMA table_info(commands)")}
        if "lease_expires_at" not in cols:
            con.execute("ALTER TABLE commands ADD COLUMN lease_expires_at INTEGER DEFAULT 0;")
        if "status" not in cols:
            con.execute("ALTER TABLE commands ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';")

        rcols = {r["name"] for r in con.execute("PRAGMA table_info(receipts)")}
        # add upgrade columns if missing
        if "status" not in rcols:
            con.execute("ALTER TABLE receipts ADD COLUMN status TEXT;")
        if "txid" not in rcols:
            con.execute("ALTER TABLE receipts ADD COLUMN txid TEXT;")
        if "fills" not in rcols:
            con.execute("ALTER TABLE receipts ADD COLUMN fills TEXT;")
        if "message" not in rcols:
            con.execute("ALTER TABLE receipts ADD COLUMN message TEXT;")

def init():
    with _conn() as con:
        con.executescript(DDL)
    _ensure_columns()

def enqueue(agent_id: str, kind: str, payload: dict, not_before: int = 0, dedupe_key: str | None = None) -> int:
    """
    Returns: new command id, or -1 if a pending duplicate (same dedupe_key) already exists.
    """
    now = int(time.time())
    with _conn() as con:
        if dedupe_key:
            # Suppress duplicates still pending or in-flight
            row = con.execute(
                "SELECT id FROM commands WHERE agent_id=? AND dedupe_key=? AND status IN ('pending','in_flight')",
                (agent_id, dedupe_key)
            ).fetchone()
            if row:
                return -1
        con.execute(
            "INSERT INTO commands(created_at,agent_id,kind,payload,not_before,dedupe_key,status,lease_expires_at) "
            "VALUES(?,?,?,?,?,?, 'pending', 0)",
            (now, agent_id, kind, json.dumps(payload), not_before, dedupe_key)
        )
        return con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

def _reap_expired(con, agent_id: str):
    """
    Move in-flight commands with expired leases back to pending (so they can be retried).
    """
    now = int(time.time())
    con.execute(
        "UPDATE commands SET status='pending', lease_expires_at=0 "
        "WHERE agent_id=? AND status='in_flight' AND lease_expires_at>0 AND lease_expires_at<=?",
        (agent_id, now)
    )

def pull(agent_id: str, limit: int = 10, lease_s: int = DEFAULT_LEASE_S):
    """
    Pull at most `limit` due commands and lease them for `lease_s` seconds.
    Returns: list of {id, created_at, kind, payload}
    NOTE: signature kept compatible with existing callers; `lease_s` is optional.
    """
    now = int(time.time())
    lease_until = now + max(1, int(lease_s))
    with _conn() as con:
        _reap_expired(con, agent_id)
        # Select pending & due
        rows = con.execute(
            "SELECT id, created_at, kind, payload FROM commands "
            "WHERE agent_id=? AND status='pending' AND (not_before=0 OR not_before<=?) "
            "ORDER BY id ASC LIMIT ?",
            (agent_id, now, limit)
        ).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            # Lease them (mark in_flight with expiration)
            qmarks = ",".join("?" * len(ids))
            con.execute(
                f"UPDATE commands SET status='in_flight', lease_expires_at=? WHERE id IN ({qmarks})",
                (lease_until, *ids)
            )
        # Return payloads
        return [
            dict(id=r["id"], created_at=r["created_at"], kind=r["kind"], payload=json.loads(r["payload"]))
            for r in rows
        ]

def ack(agent_id: str, receipts: list[dict]):
    """
    receipts: [{id:<cmd_id>, ok:bool, result:dict, status:str?, txid:str?, fills:list?, message:str?, ts:int?}]
    Idempotent: a second ack for the same (cmd_id, agent_id) will upsert the receipt and keep command status final.
    """
    now = int(time.time())
    with _conn() as con:
        for r in receipts:
            cmd_id = int(r["id"])
            ok = 1 if r.get("ok") else 0
            status = r.get("status") or ("ok" if ok else "error")
            ts = int(r.get("ts", now))
            result = json.dumps(r.get("result", {}), ensure_ascii=False)
            txid = r.get("txid")
            fills = json.dumps(r.get("fills", []), ensure_ascii=False) if isinstance(r.get("fills"), (list, tuple)) else None
            message = r.get("message")

            # Upsert receipt (idempotent on UNIQUE(cmd_id, agent_id))
            try:
                con.execute(
                  "INSERT OR IGNORE INTO receipts(cmd_id,agent_id,ok,received_at,result) VALUES(?,?,?,?,?)",
                  (cmd_id, agent_id, ok, now, result)
                )
            except sqlite3.IntegrityError:
                # update existing receipt
                con.execute(
                    "UPDATE receipts SET ok=?, status=?, received_at=?, txid=?, fills=?, message=?, result=? "
                    "WHERE cmd_id=? AND agent_id=?",
                    (ok, status, ts, txid, fills, message, result, cmd_id, agent_id)
                )

            # Finalize command status
            final = "done" if ok else ("expired" if status == "expired" else "error")
            con.execute(
                "UPDATE commands SET status=?, lease_expires_at=0 WHERE id=? AND agent_id=?",
                (final, cmd_id, agent_id)
            )
