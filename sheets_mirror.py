
#!/usr/bin/env python3
from __future__ import annotations
import os, sqlite3, json, time
from datetime import datetime, timezone

OUTBOX_DB_PATH = os.getenv("OUTBOX_DB_PATH", "/opt/render/project/src/outbox.sqlite")
RANGE_RECEIPTS = os.getenv("SHEETS_RANGE_RECEIPTS", "Rotation_Log!A2:J")
RANGE_QUEUE    = os.getenv("SHEETS_RANGE_QUEUE", "NovaHeartbeat!B2:E2")
MAX_ROWS       = int(os.getenv("SHEETS_MIRROR_MAX_ROWS", "200"))

def _utc(ts):
    if not ts and ts != 0:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return ""

def _table_exists(conn, name: str) -> bool:
    try:
        cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
        return cur.fetchone() is not None
    except Exception:
        return False

def fetch_data():
    rcpts = []
    qm = {"acked": 0, "failed": 0, "leased": 0, "queued": 0}
    try:
        conn = sqlite3.connect(OUTBOX_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        # receipts table is required
        cur.execute("""SELECT id, command_id, agent_id, status, ts, detail
                       FROM receipts ORDER BY ts DESC LIMIT ?""", (MAX_ROWS,))
        rcpts = [dict(r) for r in cur.fetchall()]

        # queue table is optional in some snapshots; try to compute if present
        if _table_exists(conn, "queue"):
            cur.execute("""SELECT
                              SUM(CASE WHEN status='acked'  THEN 1 ELSE 0 END) AS acked,
                              SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                              SUM(CASE WHEN status='leased' THEN 1 ELSE 0 END) AS leased,
                              SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued
                           FROM queue""")
            row = cur.fetchone()
            if row:
                qm.update({k: row[k] or 0 for k in row.keys()})
        conn.close()
    except Exception:
        pass
    return rcpts, qm

def fetch_queue_from_health(qm: dict) -> dict:
    bases = [
        os.getenv("RENDER_EXTERNAL_URL","").strip(),
        os.getenv("BUS_BASE_URL","").strip(),
        os.getenv("BASE_URL","").strip(),
        "http://127.0.0.1:10000",
        "http://localhost:10000",
    ]
    try:
        import requests
    except Exception:
        return qm
    for b in bases:
        if not b:
            continue
        try:
            r = requests.get(b.rstrip('/') + "/api/health/summary", timeout=8)
            if r.ok:
                j = r.json()
                q = j.get("queue", {})
                for k in ("acked","failed","leased","queued"):
                    if k in q:
                        qm[k] = q.get(k, 0) or 0
                return qm
        except Exception:
            continue
    return qm

def flatten_receipts(rcpts):
    rows = []
    for r in rcpts:
        try:
            d = json.loads(r.get("detail") or "{}")
        except Exception:
            d = {}
        p = d.get("payload") or {}
        rows.append([
            r.get("command_id",""),                           # A
            (p.get("venue") or d.get("venue") or ""),         # B
            (p.get("symbol") or d.get("symbol") or ""),       # C
            (p.get("side") or d.get("side") or ""),           # D
            p.get("executed_qty") or d.get("executed_qty") or "",  # E
            p.get("avg_price")   or d.get("avg_price")   or "",    # F
            d.get("status",""),                               # G
            p.get("note") or d.get("note") or "",             # H
            _utc(r.get("ts")),                                # I
            (p.get("mode") or d.get("mode") or ""),           # J
        ])
    return rows

# ----------- Writers ----------------

def _gw_write(range_a1, values):
    try:
        from sheets_gateway import build_gateway_from_env
        gw = build_gateway_from_env()
        gw.enqueue_write(range_a1, values)
        res = gw.flush()
        return bool(res.get("ok")), res
    except Exception as e:
        return False, f"gateway not available: {e}"

def _gspread_client():
    # Use existing env: GOOGLE_CREDS_JSON_PATH / GOOGLE_APPLICATION_CREDENTIALS / SVC_JSON
    import json, pathlib
    try:
        import gspread
    except Exception as e:
        raise RuntimeError("gspread not installed") from e
    raw = None
    for key in ("GOOGLE_CREDS_JSON_PATH","GOOGLE_APPLICATION_CREDENTIALS"):
        p = os.getenv(key, "").strip()
        if p and pathlib.Path(p).exists():
            raw = pathlib.Path(p).read_text(encoding="utf-8"); break
    if raw is None:
        svc = os.getenv("SVC_JSON","").strip()
        if svc:
            if pathlib.Path(svc).exists():
                raw = pathlib.Path(svc).read_text(encoding="utf-8")
            else:
                # assume raw json
                raw = svc
    if not raw:
        raise RuntimeError("service JSON not found in GOOGLE_CREDS_JSON_PATH / GOOGLE_APPLICATION_CREDENTIALS / SVC_JSON")
    data = json.loads(raw)
    gc = gspread.service_account_from_dict(data)  # type: ignore
    return gc

def _direct_write(range_a1, values):
    try:
        gc = _gspread_client()
        sh = gc.open_by_url(os.getenv("SHEET_URL","").strip())
        if "!" not in range_a1:
            raise RuntimeError("Range must include worksheet name, e.g., 'Sheet1!A2'")
        ws_name, rng = range_a1.split("!",1)
        ws = sh.worksheet(ws_name)
        body = {"valueInputOption":"USER_ENTERED","data":[{"range": f"{ws_name}!{rng}", "values": values}]}
        res = sh.batch_update(body)  # type: ignore
        return True, res
    except Exception as e:
        return False, f"direct gspread write failed: {e}"

def _http_write(range_a1, values):
    try:
        import requests
    except Exception as e:
        return False, f"requests missing: {e}"
    base = os.getenv("BUS_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or os.getenv("BASE_URL") or ""
    if not base:
        return False, "no BUS_BASE_URL/RENDER_EXTERNAL_URL/BASE_URL set"
    try:
        r = requests.post(base.rstrip("/") + "/sheets/enqueue", json={"range": range_a1, "values": values}, timeout=20)
        if not r.ok:
            return False, {"status": r.status_code, "body": r.text}
        r2 = requests.post(base.rstrip("/") + "/sheets/flush", timeout=20)
        return r2.ok, {"enqueue": r.json(), "flush": r2.json()}
    except Exception as e:
        return False, str(e)

def write_values(range_a1, values):
    # 1) try local gateway
    ok, res = _gw_write(range_a1, values)
    if ok: return ok, res
    # 2) try direct gspread
    ok, res = _direct_write(range_a1, values)
    if ok: return ok, res
    # 3) try HTTP to /sheets endpoints
    return _http_write(range_a1, values)

def main():
    rcpts, qm = fetch_data()
    # If queue metrics missing (no queue table), fetch from /api/health/summary
    if not any(qm.values()):
        qm = fetch_queue_from_health(qm)

    rows = flatten_receipts(rcpts)

    # Write receipts
    ok1, res1 = write_values(RANGE_RECEIPTS, rows if rows else [["", "", "", "", "", "", "", "", "", ""]])
    # Write queue metrics (acked, failed, leased, queued)
    qrow = [[qm.get("acked",0), qm.get("failed",0), qm.get("leased",0), qm.get("queued",0)]]
    ok2, res2 = write_values(RANGE_QUEUE, qrow)

    print(json.dumps({"ok": bool(ok1 and ok2), "rows": len(rows), "queue": qrow, "res1": str(res1)[:200], "res2": str(res2)[:200]}))

if __name__ == "__main__":
    main()
