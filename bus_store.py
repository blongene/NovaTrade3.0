# bus_store.py
import os, sqlite3, time, json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

DB_PATH = os.getenv("OUTBOX_DB_PATH", "./outbox.sqlite")

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',     -- queued|leased|acked|failed
  created_at TEXT NOT NULL,
  leased_at TEXT,
  lease_expires_at TEXT,
  agent_id TEXT
);
CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id TEXT NOT NULL,
  agent_id TEXT,
  status TEXT NOT NULL,                      -- ok|error|skipped
  detail TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(command_id) REFERENCES commands(id)
);
CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status);
CREATE INDEX IF NOT EXISTS idx_commands_lease_exp ON commands(lease_expires_at);
"""

def _connect():
  conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
  conn.execute("PRAGMA foreign_keys=ON;")
  return conn

def init_db():
  conn = _connect()
  try:
    for stmt in SCHEMA.strip().split(";"):
      s = stmt.strip()
      if s:
        conn.execute(s)
  finally:
    conn.close()

def enqueue_command(cmd_id: str, payload: Dict) -> None:
  now = datetime.utcnow().isoformat()
  conn = _connect()
  try:
    conn.execute(
      "INSERT INTO commands(id, payload, status, created_at) VALUES (?,?, 'queued', ?)",
      (cmd_id, json.dumps(payload, separators=(",",":")), now)
    )
  finally:
    conn.close()

def pull_commands(agent_id: str, max_items: int = 10, lease_seconds: int = 90) -> List[Dict]:
  now = datetime.utcnow()
  now_iso = now.isoformat()
  lease_expiry_iso = (now + timedelta(seconds=lease_seconds)).isoformat()

  conn = _connect()
  try:
    # pick available commands: queued or expired lease
    rows = conn.execute(
      """
      SELECT id, payload FROM commands
      WHERE status IN ('queued','leased')
        AND (lease_expires_at IS NULL OR lease_expires_at < ?)
      ORDER BY created_at ASC
      LIMIT ?
      """,
      (now_iso, max_items)
    ).fetchall()

    ids = [r[0] for r in rows]
    if not ids:
      return []

    # lease them atomically
    qmarks = ",".join("?" for _ in ids)
    conn.execute(
      f"""
      UPDATE commands
         SET status='leased',
             leased_at=?,
             lease_expires_at=?,
             agent_id=?
       WHERE id IN ({qmarks})
      """,
      (now_iso, lease_expiry_iso, agent_id, *ids)
    )

    return [{"id": r[0], "payload": json.loads(r[1])} for r in rows]
  finally:
    conn.close()

def renew_lease(cmd_id: str, lease_seconds: int = 90) -> None:
  lease_expires = (datetime.utcnow() + timedelta(seconds=lease_seconds)).isoformat()
  conn = _connect()
  try:
    conn.execute(
      "UPDATE commands SET lease_expires_at=? WHERE id=? AND status='leased'",
      (lease_expires, cmd_id)
    )
  finally:
    conn.close()

def ack_command(cmd_id: str, agent_id: str, status: str, detail: Optional[Dict] = None) -> None:
  now = datetime.utcnow().isoformat()
  conn = _connect()
  try:
    conn.execute(
      "INSERT INTO receipts(command_id, agent_id, status, detail, created_at) VALUES (?,?,?,?,?)",
      (cmd_id, agent_id, status.lower(), json.dumps(detail or {}, separators=(",",":")), now)
    )
    conn.execute("UPDATE commands SET status='acked' WHERE id=?", (cmd_id,))
  finally:
    conn.close()

def queue_depth() -> Dict[str, int]:
  conn = _connect()
  try:
    out = {}
    for st in ("queued","leased","acked","failed"):
      n = conn.execute("SELECT COUNT(1) FROM commands WHERE status=?", (st,)).fetchone()[0]
      out[st] = int(n)
    return out
  finally:
    conn.close()
