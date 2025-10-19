# receipts_bridge.py — mirror DB receipts into Google Sheets (Trade_Log)
# Run this as a scheduled job on Bus every few minutes.
#
# Env:
#   OUTBOX_DB_PATH=/data/outbox.db
#   SHEET_URL=...
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json  (or your existing auth flow)
import os, json, sqlite3, time
from datetime import datetime

try:
    from utils import get_sheet
except Exception:
    get_sheet = None

DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")

def load_new_receipts(last_id_path="/data/receipt_checkpoint.txt"):
    os.makedirs(os.path.dirname(last_id_path), exist_ok=True)
    last_id = 0
    if os.path.exists(last_id_path):
        try:
            last_id = int(open(last_id_path).read().strip() or "0")
        except Exception:
            last_id = 0
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    cur = c.cursor()
    cur.execute("select id, ts, payload from receipts where id > ? order by id asc", [last_id])
    rows = cur.fetchall()
    c.close()
    return last_id, rows, last_id_path

def append_trade_log(rows):
    if not get_sheet:
        print("[bridge] get_sheet() unavailable; skipping Trade_Log write.")
        return 0, 0
    sheet = get_sheet()
    ws = sheet.worksheet("Trade_Log")
    ok = 0; err = 0
    for rid, ts, payload in rows:
        try:
            j = json.loads(payload)
            rec = j.get("receipt", {})
            ts_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            row = [
                ts_utc,
                rec.get("symbol",""),
                rec.get("venue",""),
                rec.get("side",""),
                str(rec.get("amount_quote","")),
                (j.get("status","") or "").upper(),
                rec.get("reason",""),
                f"cmd:{j.get('id','')}",
                f"rcp:{rid}",
            ]
            ws.append_row(row)
            ok += 1
        except Exception as e:
            print("[bridge] append error:", e)
            err += 1
    return ok, err

if __name__ == "__main__":
    last_id, rows, path = load_new_receipts()
    if not rows:
        print("[bridge] no new receipts")
        raise SystemExit(0)
    ok, err = append_trade_log(rows)
    if ok:
        new_last = rows[-1][0]
        with open(path, "w") as fp:
            fp.write(str(new_last))
    print(f"[bridge] done ok={ok} err={err}")
