
#!/usr/bin/env python3
# receipts_compactor.py — roll up old receipts into daily summary and prune raws.
from __future__ import annotations
import os, sqlite3, json, time

OUTBOX_DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/legacy_bus.sqlite")
RETENTION_DAYS = int(os.getenv("RECEIPTS_RETENTION_DAYS", "14"))
MAX_DELETE = int(os.getenv("COMPACTOR_MAX_DELETE", "5000"))

def _exec(conn, sql, args=()):
    cur = conn.cursor()
    cur.execute(sql, args)
    return cur

def ensure_schema(conn):
    _exec(conn, """
        CREATE TABLE IF NOT EXISTS receipts_daily (
            day_utc TEXT PRIMARY KEY,
            count_total INTEGER NOT NULL DEFAULT 0,
            count_ok INTEGER NOT NULL DEFAULT 0,
            count_error INTEGER NOT NULL DEFAULT 0,
            last_ts REAL
        )
    """)

def compact_once():
    conn = sqlite3.connect(OUTBOX_DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    cutoff = time.time() - RETENTION_DAYS * 86400

    rows = _exec(conn, """
        SELECT date(ts, 'unixepoch') as day_utc,
               COUNT(*) as total,
               SUM(CASE WHEN status='ok' THEN 1 ELSE 0 END) as okc,
               SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) as errc,
               MAX(ts) as last_ts
        FROM receipts
        WHERE ts < ?
        GROUP BY day_utc
    """, (cutoff,)).fetchall()

    for r in rows:
        _exec(conn, """
            INSERT INTO receipts_daily(day_utc, count_total, count_ok, count_error, last_ts)
            VALUES (?,?,?,?,?)
            ON CONFLICT(day_utc) DO UPDATE SET
              count_total = receipts_daily.count_total + excluded.count_total,
              count_ok    = receipts_daily.count_ok    + excluded.count_ok,
              count_error = receipts_daily.count_error + excluded.count_error,
              last_ts     = MAX(receipts_daily.last_ts, excluded.last_ts)
        """, (r["day_utc"], r["total"], r["okc"] or 0, r["errc"] or 0, r["last_ts"]))

    _exec(conn, "DELETE FROM receipts WHERE ts < ? LIMIT ?", (cutoff, MAX_DELETE))
    conn.commit()
    try:
        _exec(conn, "VACUUM")
    except Exception:
        pass

    conn.close()
    print(json.dumps({"ok": True, "rolled_days": len(rows), "deleted_lte": MAX_DELETE, "cutoff": cutoff}))

if __name__ == "__main__":
    compact_once()
