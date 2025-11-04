
#!/usr/bin/env python3
# sheets_mirror_plus.py — publish a compact Performance_Dashboard row each run.
from __future__ import annotations
import os, sqlite3, json
from datetime import datetime, timezone

OUTBOX_DB_PATH = os.getenv("OUTBOX_DB_PATH", "/opt/render/project/src/outbox.sqlite")
RANGE_PD = os.getenv("SHEETS_RANGE_PD_SUMMARY", "Performance_Dashboard!A2:F")
MAX_ROWS = int(os.getenv("SHEETS_MIRROR_MAX_ROWS", "500"))

def _utc(ts):
    if ts is None or ts == "":
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return ""

def _fetch_receipts():
    rows = []
    try:
        conn = sqlite3.connect(OUTBOX_DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT status, ts, detail FROM receipts ORDER BY ts DESC LIMIT ?", (MAX_ROWS,))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
    except Exception:
        pass
    return rows

def summarize():
    rcpts = _fetch_receipts()
    ok_cnt = sum(1 for r in rcpts if (r.get("status") or "").lower() == "ok")
    err_cnt = sum(1 for r in rcpts if (r.get("status") or "").lower() == "error")
    last_ok = next((_utc(r.get("ts")) for r in rcpts if (r.get("status") or "").lower() == "ok"), "")
    last_err = next((_utc(r.get("ts")) for r in rcpts if (r.get("status") or "").lower() == "error"), "")
    ts_now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return [[ts_now, len(rcpts), ok_cnt, err_cnt, last_ok, last_err]]

def _gspread_client():
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
                raw = svc
    if not raw:
        raise RuntimeError("service JSON not found in GOOGLE_CREDS_JSON_PATH / GOOGLE_APPLICATION_CREDENTIALS / SVC_JSON")
    data = json.loads(raw)
    import gspread
    gc = gspread.service_account_from_dict(data)  # type: ignore
    return gc


def _direct_write(range_a1, values):
    try:
        gc = _gspread_client()
        sh = gc.open_by_url(os.getenv("SHEET_URL","").strip())
        if "!" not in range_a1:
            raise RuntimeError("Range must include worksheet name, e.g., 'Sheet1!A2'")
        # Use gspread's values_update (Values API), not batchUpdate (Drive batchUpdate schema)
        res = sh.values_update(
            range_a1,
            params={"valueInputOption": "USER_ENTERED"},
            body={"values": values},
        )
        return True, res
    except Exception as e:
        return False, f"direct gspread write failed: {e}"

def _gw_write(range_a1, values):
(range_a1, values):
    try:
        from sheets_gateway import build_gateway_from_env
        gw = build_gateway_from_env()
        gw.enqueue_write(range_a1, values)
        res = gw.flush()
        return bool(res.get("ok")), res
    except Exception as e:
        return False, f"gateway not available: {e}"

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
    if os.getenv("FORCE_GSPREAD_DIRECT","").lower() in {"1","true","yes"}:
        return _direct_write(range_a1, values)
    ok, res = _gw_write(range_a1, values)
    if ok: return ok, res
    ok, res = _direct_write(range_a1, values)
    if ok: return ok, res
    return _http_write(range_a1, values)

def main():
    row = summarize()
    ok, res = write_values(RANGE_PD, row)
    print(json.dumps({"ok": bool(ok), "row": row, "res": str(res)[:200]}))

if __name__ == "__main__":
    main()
