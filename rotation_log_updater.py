# rotation_log_updater.py
import os, time
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---- Safe imports from utils (with graceful fallbacks) -----------------------
try:
    from utils import with_sheet_backoff, send_telegram_message_dedup, str_or_empty, to_float
except Exception:  # fallback if utils isn’t available in local test context
    def with_sheet_backoff(fn):
        def wrapper(*a, **k):
            return fn(*a, **k)
        return wrapper
    def send_telegram_message_dedup(msg, key="rotation_log_updater", ttl_min=15):
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

def _header_index_map(header_row):
    return {str_or_empty(h): i for i, h in enumerate(header_row, start=1)}

def _find_numeric_roi(rec: dict):
    """Return a numeric ROI from common keys if present; else None."""
    for key in ("ROI %", "ROI", "Follow-up ROI", "Follow up ROI", "Final ROI"):
        if key in rec:
            v = to_float(rec.get(key))
            if v is not None:
                return v
    return None

def _load_records(ws):
    hdr = ws.row_values(1)
    idx = _header_index_map(hdr)
    rows = ws.get_all_records()
    return hdr, idx, rows

def _cell_address(col_idx, row_idx):  # 1-based col,row
    # Convert to A1 (supports up to ZZZ easily)
    n = col_idx
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row_idx}"

def run_rotation_log_updater():
    """
    Patches Rotation_Log follow-up ROI cells using ROI_Review_Log
    – Only writes strictly numeric ROI (skips text like '0d since vote')
    – Batches all writes
    """
    sheet = _get_sheet()

    rl_ws = _ws(sheet, "Rotation_Log")
    rr_ws = _ws(sheet, "ROI_Review_Log")

    rl_hdr, rl_idx, rl_rows = _load_records(rl_ws)
    rr_hdr, rr_idx, rr_rows = _load_records(rr_ws)

    # Map best numeric ROI per token from ROI_Review_Log
    review_roi_by_token = {}
    for r in rr_rows:
        token = str_or_empty(r.get("Token"))
        roi = _find_numeric_roi(r)
        if token and roi is not None:
            # keep the most recent or largest; here we just keep the latest seen
            review_roi_by_token[token] = roi

    target_col_name = "Follow-up ROI" if "Follow-up ROI" in rl_idx else (
        "ROI %" if "ROI %" in rl_idx else None
    )
    if not target_col_name:
        send_telegram_message_dedup("⚠️ rotation_log_updater: no suitable ROI column found in Rotation_Log", "rotation_log_updater_no_col", 60)
        return

    write_col = rl_idx[target_col_name]
    writes = []

    # Iterate Rotation_Log and build updates where target cell is non-numeric/blank and we have numeric ROI
    for row_offset, rec in enumerate(rl_rows, start=2):
        token = str_or_empty(rec.get("Token"))
        if not token:
            continue

        current_val = rec.get(target_col_name)
        current_num = to_float(current_val)
        if current_num is not None:
            continue  # already numeric, leave as-is

        candidate = review_roi_by_token.get(token)
        if candidate is None:
            continue  # no numeric ROI available

        # Prepare single-cell update
        a1 = _cell_address(write_col, row_offset)
        writes.append({"range": f"Rotation_Log!{a1}", "values": [[candidate]]})

    if writes:
        # Batch update
        rl_ws.batch_update(writes, value_input_option="RAW")
        send_telegram_message_dedup(f"🧩 Rotation_Log patched {len(writes)} follow-up ROI cell(s).", "rotation_log_updater", 15)
    else:
        print("rotation_log_updater: nothing to patch (all numeric or no candidates).")

if __name__ == "__main__":
    run_rotation_log_updater()
