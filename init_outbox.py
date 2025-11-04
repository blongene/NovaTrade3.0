#!/usr/bin/env python3
"""
init_outbox.py — ensures outbox.sqlite schema exists
(run once on boot or manually from shell)
"""
import sqlite3, os

DB = os.getenv("OUTBOX_DB_PATH", "/opt/render/project/src/outbox.sqlite")
os.makedirs(os.path.dirname(DB), exist_ok=True)

schema = """
CREATE TABLE IF NOT EXISTS queue (
    id TEXT PRIMARY KEY,
    payload TEXT,
    lease_expiry REAL,
    acked INTEGER DEFAULT 0,
    failed INTEGER DEFAULT 0,
    created REAL DEFAULT (strftime('%s','now'))
);
CREATE TABLE IF NOT EXISTS receipts (
    id TEXT PRIMARY KEY,
    command_id TEXT,
    payload TEXT,
    status TEXT,
    ts REAL DEFAULT (strftime('%s','now'))
);
CREATE TABLE IF NOT EXISTS commands (
    id TEXT PRIMARY KEY,
    payload TEXT,
    status TEXT,
    created REAL DEFAULT (strftime('%s','now'))
);
"""

conn = sqlite3.connect(DB)
conn.executescript(schema)
conn.commit()
conn.close()
print(f"✅ ensured schema in {DB}")
