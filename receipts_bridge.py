# receipts_bridge.py — mirror DB receipts into Google Sheets (Trade_Log)
# Run this as a scheduled job on Bus every few minutes.
#
# Env:
#   OUTBOX_DB_PATH=/data/outbox.db
#   SHEET_URL=...
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json  (or your existing auth flow)
import time, gspread, json, sqlite3, os
from datetime import datetime
from utils import sheets_append_rows, backoff_guard

WRITE_BATCH_SIZE = 5          # rows per write
WRITE_COOLDOWN_SEC = 8        # pause between writes
MAX_RETRIES = 5

def run_once():
    db = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
    sheet_url = os.getenv("SHEET_URL")
    ws_name = "Trade_Log"
    if not sheet_url:
        print("[bridge] SHEET_URL missing")
        return

    con = sqlite3.connect(db)
    cur = con.cursor()
    cur.execute("select id, payload from receipts order by id asc")
    rows = cur.fetchall()
    con.close()

    print(f"[bridge] syncing {len(rows)} receipts → {ws_name}")

    # Prepare data for writing
    batch, written = [], 0
    for rid, payload in rows:
        j = json.loads(payload)
        rcp = j.get("receipt", {})
        data_row = [
            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(rcp.get("ts", 0))),
            rcp.get("venue"),
            rcp.get("symbol"),
            rcp.get("side"),
            rcp.get("amount_quote"),
            rcp.get("executed_qty"),
            rcp.get("avg_price"),
            j.get("status"),
            rcp.get("note"),
            j.get("id"),
            rid,
        ]
        batch.append(data_row)

        # When batch full → write
        if len(batch) >= WRITE_BATCH_SIZE:
            for attempt in range(MAX_RETRIES):
                try:
                    sheets_append_rows(sheet_url, ws_name, batch)
                    print(f"[bridge] wrote batch of {len(batch)} rows")
                    written += len(batch)
                    batch.clear()
                    time.sleep(WRITE_COOLDOWN_SEC)
                    break
                except Exception as e:
                    print(f"[bridge] batch write error: {e}")
                    time.sleep(WRITE_COOLDOWN_SEC * (attempt + 1))

    if batch:
        try:
            sheets_append_rows(sheet_url, ws_name, batch)
            written += len(batch)
        except Exception as e:
            print(f"[bridge] final batch error: {e}")

    print(f"[bridge] done ok={written}")

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
