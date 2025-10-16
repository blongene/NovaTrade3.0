# telemetry_store.py — Bus-side SQLite store for telemetry pushes + heartbeats
import os, sqlite3, json, time
from typing import Dict, Any

DB_PATH = os.getenv("BUS_TELEMETRY_DB", "bus_telemetry.db")

PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA foreign_keys=ON;",
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry_push (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent TEXT,
  ts INTEGER,
  aggregates_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_tp_agent_ts ON telemetry_push(agent, ts);

CREATE TABLE IF NOT EXISTS telemetry_heartbeat (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent TEXT,
  ts INTEGER,
  latency_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_hb_agent_ts ON telemetry_heartbeat(agent, ts);
"""

def _conn():
    first = not os.path.exists(DB_PATH)
    con = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    con.row_factory = sqlite3.Row
    for p in PRAGMAS: con.execute(p)
    if first:
        for stmt in filter(None, SCHEMA.split(";")):
            s = stmt.strip()
            if s: con.execute(s + ";")
    return con

def store_push(*, agent: str, ts: int, aggregates: Dict[str, Any]):
    con = _conn()
    con.execute(
        "INSERT INTO telemetry_push(agent, ts, aggregates_json) VALUES(?,?,?)",
        (agent, int(ts), json.dumps(aggregates, separators=(",", ":"), ensure_ascii=False)),
    )

def store_heartbeat(*, agent: str, ts: int, latency_ms: int):
    con = _conn()
    con.execute(
        "INSERT INTO telemetry_heartbeat(agent, ts, latency_ms) VALUES(?,?,?)",
        (agent, int(ts), int(latency_ms)),
    )
