
#!/usr/bin/env python3
from __future__ import annotations
import os, sqlite3, time, json
from datetime import datetime, timezone

# ---------- Config from existing envs ----------
OUTBOX_DB_PATH = os.getenv("OUTBOX_DB_PATH", "/opt/render/project/src/outbox.sqlite")

# target ranges (customize via env if desired)
RANGE_RECEIPTS = os.getenv("SHEETS_RANGE_RECEIPTS", "Rotation_Log!A2:J")
RANGE_QUEUE    = os.getenv("SHEETS_RANGE_QUEUE", "NovaHeartbeat!B2:E2")

# batch limits
MAX_ROWS = int(os.getenv("SHEETS_MIRROR_MAX_ROWS", "200"))
TTL_SEC  = int(os.getenv("SHEETS_MIRROR_TTL_SEC", "600"))

# ---------- Helpers ----------
def _utc(ts: float | int | None) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return ""

def fetch_data():
    conn = sqlite3.connect(OUTBOX_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Receipts (latest first)
    cur.execute("""        SELECT id, command_id, agent_id, status, ts, detail
        FROM receipts
        ORDER BY ts DESC
        LIMIT ?
    """, (MAX_ROWS,))
    rcpts = [dict(r) for r in cur.fetchall()]

    # Queue metrics (counts)
    cur.execute("""        SELECT
            SUM(CASE WHEN status='acked' THEN 1 ELSE 0 END) AS acked,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status='leased' THEN 1 ELSE 0 END) AS leased,
            SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued
        FROM (SELECT status FROM queue)
    """)
    qm = dict(cur.fetchone() or {})

    conn.close()
    return rcpts, qm

def flatten_receipts(rcpts):
    rows = []
    for r in rcpts:
        try:
            d = json.loads(r.get("detail") or "{}")
        except Exception:
            d = {}
        p = d.get("payload") or {}

        rows.append([
            r.get("command_id",""),                          # A: command_id
            (p.get("venue") or d.get("venue") or ""),        # B: venue
            (p.get("symbol") or d.get("symbol") or ""),      # C: symbol
            (p.get("side") or d.get("side") or ""),          # D: side
            p.get("executed_qty") or d.get("executed_qty") or "",  # E: qty
            p.get("avg_price") or d.get("avg_price") or "",        # F: avg_price
            d.get("status",""),                              # G: status (if present)
            p.get("note") or d.get("note") or "",            # H: note
            _utc(r.get("ts")),                               # I: ts (receipt)
            (p.get("mode") or d.get("mode") or ""),          # J: mode
        ])
    return rows

# ---------- Sheets enqueue/flush (in-process) ----------
def use_gateway_enq(range_a1, values):
    try:
        from sheets_gateway import build_gateway_from_env
    except Exception as e:
        return False, f"import sheets_gateway failed: {e}"
    gw = build_gateway_from_env()
    gw.enqueue_write(range_a1, values)
    res = gw.flush()
    ok = bool(res.get("ok"))
    return ok, res

# ---------- Sheets via HTTP (fallback) ----------
def use_http_enq(range_a1, values):
    import requests
    base = os.getenv("BUS_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or os.getenv("BASE_URL") or "http://127.0.0.1:10000"
    try:
        r = requests.post(base.rstrip("/") + "/sheets/enqueue", json={"range": range_a1, "values": values}, timeout=20)
        if r.status_code != 200:
            return False, {"status": r.status_code, "body": r.text}
        r2 = requests.post(base.rstrip("/") + "/sheets/flush", timeout=20)
        return (r2.status_code == 200), {"enqueue": r.json(), "flush": r2.json()}
    except Exception as e:
        return False, str(e)

def main():
    rcpts, qm = fetch_data()

    # Prepare values
    rows = flatten_receipts(rcpts)

    # If too few rows and last mirror was recent, skip (lightweight guard)
    if len(rows) == 0:
        print(json.dumps({"ok": True, "msg": "no data to mirror"}))
        return

    # Enqueue write & flush (prefer in-process)
    ok, res = use_gateway_enq(RANGE_RECEIPTS, rows)
    if not ok:
        ok, res = use_http_enq(RANGE_RECEIPTS, rows)

    # Also push queue metrics to a single row (acked, failed, leased, queued)
    qrow = [[qm.get("acked",0), qm.get("failed",0), qm.get("leased",0), qm.get("queued",0)]]
    ok2, res2 = use_gateway_enq(RANGE_QUEUE, qrow) if ok else use_http_enq(RANGE_QUEUE, qrow)

    print(json.dumps({"ok": bool(ok and ok2), "receipts_rows": len(rows), "queue_row": qrow, "res": res, "res2": res2}, default=str))

if __name__ == "__main__":
    main()
