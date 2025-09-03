# outbox_db.py — tiny SQLite outbox for commands + receipts (cloud)
# in outbox_db.py before sqlite connect
import os
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

import sqlite3, json, time
from contextlib import contextmanager
DB_PATH = os.getenv("OUTBOX_DB_PATH", "/tmp/outbox.db")

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
  status TEXT NOT NULL DEFAULT 'pending'  -- pending|acked
);
CREATE INDEX IF NOT EXISTS ix_cmd_agent ON commands(agent_id, status);

CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cmd_id INTEGER NOT NULL,
  agent_id TEXT NOT NULL,
  ok INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  result TEXT NOT NULL,
  FOREIGN KEY(cmd_id) REFERENCES commands(id)
);
"""

@contextmanager
def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    yield con
    con.close()

def init():
    with _conn() as con:
        con.executescript(DDL)
        con.commit()

def enqueue(agent_id: str, kind: str, payload: dict, not_before: int = 0, dedupe_key: str | None = None) -> int:
    with _conn() as con:
        if dedupe_key:
            # suppress duplicates still pending
            cur = con.execute("SELECT id FROM commands WHERE agent_id=? AND status='pending' AND dedupe_key=?",
                              (agent_id, dedupe_key))
            if cur.fetchone():
                return -1
        con.execute("INSERT INTO commands(created_at,agent_id,kind,payload,not_before,dedupe_key) VALUES(?,?,?,?,?,?)",
                    (int(time.time()), agent_id, kind, json.dumps(payload), not_before, dedupe_key))
        con.commit()
        return con.execute("SELECT last_insert_rowid()").fetchone()[0]

def pull(agent_id: str, limit: int = 10):
    now = int(time.time())
    with _conn() as con:
        cur = con.execute("""
          SELECT id, created_at, kind, payload
          FROM commands
          WHERE agent_id=? AND status='pending' AND (not_before=0 OR not_before<=?)
          ORDER BY id ASC LIMIT ?""", (agent_id, now, limit))
        return [dict(id=r["id"], created_at=r["created_at"], kind=r["kind"], payload=json.loads(r["payload"])) for r in cur.fetchall()]

def ack(agent_id: str, receipts: list[dict]):
    """
    receipts: [{id:<cmd_id>, ok:bool, result:dict}]
    """
    now = int(time.time())
    with _conn() as con:
        for r in receipts:
            cmd_id = int(r["id"])
            ok = 1 if r.get("ok") else 0
            result = json.dumps(r.get("result", {}))
            con.execute("INSERT INTO receipts(cmd_id,agent_id,ok,received_at,result) VALUES(?,?,?,?,?)",
                        (cmd_id, agent_id, ok, now, result))
            con.execute("UPDATE commands SET status='acked' WHERE id=? AND agent_id=?", (cmd_id, agent_id))
        con.commit()
