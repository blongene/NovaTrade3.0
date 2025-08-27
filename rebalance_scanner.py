# rebalance_scanner.py
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

try:
    from utils import with_sheet_backoff, send_telegram_message_dedup, str_or_empty, to_float
except Exception:
    def with_sheet_backoff(fn):
        def wrapper(*a, **k):
            return fn(*a, **k)
        return wrapper
    def send_telegram_message_dedup(msg, key="rebalance_scanner", ttl_min=15):
        print(f"[TG:{key}] {msg}")
    def str_or_empty(v):
        return str(v).strip() if v is not None else ""
    def to_float(v):
        s = str_or_empty(v).replace("%", "").replace(",", "")
        try:
            return float(s) if s != "" else None
        except Exception:
            return None

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def _get_sheet():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_url(os.getenv("SHEET_URL"))

@with_sheet_backoff
def _ws(sheet, name):
    return sheet.worksheet(name)

def _cell_address(col_idx, row_idx):
    n = col_idx
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row_idx}"

def run_rebalance_scanner():
    """
    Computes Drift = Current% - Target% for Portfolio_Targets
    Writes Drift via single batch update (no per-cell loops)
    Avoids undefined 'score' usage; purely numeric-safe parsing.
    """
    sheet = _get_sheet()
    ws = _ws(sheet, "Portfolio_Targets")

    header = ws.row_values(1)
    hmap = {str_or_empty(h): i for i, h in enumerate(header, start=1)}

    required = ["Token", "Target %", "Current %", "Drift"]
    missing = [c for c in required if c not in hmap]
    if missing:
        send_telegram_message_dedup(f"⚠️ rebalance_scanner: missing columns: {', '.join(missing)}",
                                    "rebalance_missing_cols", 60)
        return

    # Read all records once
    rows = ws.get_all_records()

    drift_updates = []
    for r_idx, rec in enumerate(rows, start=2):
        token = str_or_empty(rec.get("Token"))
        if not token:
            continue

        tgt = to_float(rec.get("Target %")) or 0.0
        cur = to_float(rec.get("Current %")) or 0.0
        drift = cur - tgt  # signed % points

        drift_col = hmap["Drift"]
        a1 = _cell_address(drift_col, r_idx)
        drift_updates.append({"range": f"Portfolio_Targets!{a1}", "values": [[f"{drift:.2f}%"]]})

    if drift_updates:
        ws.batch_update(drift_updates, value_input_option="RAW")
        send_telegram_message_dedup(f"📊 Rebalance scan updated Drift for {len(drift_updates)} token(s).",
                                    "rebalance_scan", 15)
    else:
        print("rebalance_scanner: nothing to update.")

if __name__ == "__main__":
    run_rebalance_scanner()
