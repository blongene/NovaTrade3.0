#!/usr/bin/env python3
"""init_outbox.py — ensure outbox.sqlite schema exists (idempotent)"""
import os, sqlite3

DB = os.getenv("OUTBOX_DB_PATH", "/data/legacy_bus.sqlite")
os.makedirs(os.path.dirname(DB), exist_ok=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id TEXT PRIMARY KEY,
    payload TEXT,
    lease_expiry REAL,
    acked INTEGER DEFAULT 0,
    failed INTEGER DEFAULT 0,
    created REAL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_queue_acked ON queue(acked);
CREATE INDEX IF NOT EXISTS idx_queue_lease ON queue(lease_expiry);

CREATE TABLE IF NOT EXISTS receipts (
    id TEXT PRIMARY KEY,
    command_id TEXT,
    payload TEXT,
    status TEXT,
    ts REAL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_receipts_ts ON receipts(ts);
CREATE INDEX IF NOT EXISTS idx_receipts_status ON receipts(status);

CREATE TABLE IF NOT EXISTS commands (
    id TEXT PRIMARY KEY,
    payload TEXT,
    status TEXT,
    created REAL DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_commands_created ON commands(created);
"""

conn = sqlite3.connect(DB)
conn.executescript(SCHEMA)
conn.commit()
conn.close()
print(f"✅ ensured schema in {DB}")
