# stalled_asset_detector.py
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import (
    with_sheet_backoff,
    str_or_empty,
    safe_float,          # or to_float if that’s your helper
    send_telegram_message_dedup,  # optional; comment out if not used
)

SHEET_URL  = os.getenv("SHEET_URL")
LOG_SHEET  = "Rotation_Log"
SCOPE      = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

THRESHOLD_DAYS = 7  # example stall threshold

@with_sheet_backoff
def _open_ws(title: str):
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet(title)

@with_sheet_backoff
def _get_all_values(ws):
    return ws.get_all_values()

@with_sheet_backoff
def _batch_update(ws, payload):
    if payload:
        ws.batch_update(payload, value_input_option="USER_ENTERED")

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s

def run_stalled_asset_detector():
    print("✅ Stalled Asset Detector starting…")

    try:
        ws = _open_ws(LOG_SHEET)
        vals = _get_all_values(ws)
        if not vals:
            print("ℹ️ Rotation_Log is empty.")
            return

        header = vals[0]
        hix = {h: i for i, h in enumerate(header)}  # 0-based

        # Ensure Status column exists
        updates_header = False
        if "Status" not in hix:
            header.append("Status")
            hix["Status"] = len(header) - 1
            ws.update("A1", [header])
            updates_header = True

        # Find columns we need (make optional where possible)
        token_ix     = hix.get("Token")
        last_move_ix = hix.get("Last Move Date") or hix.get("Last Move")
        status_ix    = hix.get("Status")

        if token_ix is None or last_move_ix is None or status_ix is None:
            print("⚠️ Rotation_Log missing required columns (Token / Last Move Date / Status).")
            return

        now = datetime.utcnow()
        updates = []
        alerts  = []

        for r_idx, row in enumerate(vals[1:], start=2):
            token = str_or_empty(row[token_ix]).upper()
            if not token:
                continue

            last_raw = str_or_empty(row[last_move_ix])
            days = None
            if last_raw:
                try:
                    # best effort parse: accept 'YYYY-MM-DD' or ISO-ish
                    # If parse fails, treat as unknown and skip
                    ts = last_raw.replace("T", " ").replace("Z", "")
                    dt = datetime.fromisoformat(ts)
                    days = (now - dt).days
                except Exception:
                    pass

            if days is not None and days >= THRESHOLD_DAYS:
                # flag as stalled
                a1 = f"{_col_letter(status_ix+1)}{r_idx}"
                updates.append({"range": a1, "values": [["⚠️ Stalled"]]})
                alerts.append(f"• {token} stalled ({days}d since last move)")

        if updates:
            _batch_update(ws, updates)
            print(f"✅ Stalled detector updated {len(updates)} row(s).")
        else:
            print("ℹ️ Stalled detector: nothing to update.")

        if alerts:
            body = "🛑 <b>Stalled Assets</b>\n" + "\n".join(alerts)
            try:
                send_telegram_message_dedup(body, key="stalled_assets", ttl_min=30)
            except Exception:
                pass

    except Exception as e:
        print(f"❌ Stalled detector error: {e}")
