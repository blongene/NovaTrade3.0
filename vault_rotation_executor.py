# vault_rotation_executor.py
import os
from datetime import datetime
import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

from utils import with_sheet_backoff, str_or_empty, safe_float

SHEET_URL = os.getenv("SHEET_URL")
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

@with_sheet_backoff
def _open_sheet():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

@with_sheet_backoff
def _get_ws(title: str):
    sh = _open_sheet()
    return sh.worksheet(title)

def run_vault_rotation_executor():
    print("▶️ Vault rotation executor …")
    try:
        # Only read what we need once (typical: read Token_Vault decisions)
        vault_ws = _get_ws("Token_Vault")
        log_ws   = _get_ws("Vault_Rotation_Log") if "Vault_Rotation_Log" in [w.title for w in _open_sheet().worksheets()] else None

        rows = vault_ws.get_all_records()
        # Find candidates: Decision == "ROTATE" and not yet logged today
        today = datetime.utcnow().date().isoformat()
        already = set()
        if log_ws:
            log_vals = log_ws.get_all_values()
            if log_vals:
                for r in log_vals[1:]:
                    # Expect cols: Date | Token | Action ...
                    if r and len(r) >= 2 and r[0].startswith(today):
                        already.add(str_or_empty(r[1]).strip().upper())

        to_log = []
        for r in rows:
            t = str_or_empty(r.get("Token")).strip().upper()
            decision = str_or_empty(r.get("Decision")).strip().upper()
            if not t or decision != "ROTATE":
                continue
            if t in already:
                continue
            to_log.append(t)

        if not to_log:
            print("✅ Vault rotation execution complete. 0 token(s) logged.")
            return

        # Batch append minimal rows
        if not log_ws:
            # create on the fly if missing
            sh = _open_sheet()
            log_ws = sh.add_worksheet(title="Vault_Rotation_Log", rows=1000, cols=6)
            log_ws.update("A1", [["Date", "Token", "Action", "Notes"]])

        append_rows = [[f"{today}T00:00:00Z", t, "ROTATED", "Auto-exec"] for t in to_log]
        # Use update with dynamic range
        start_row = len(log_ws.get_all_values()) + 1
        end_row = start_row + len(append_rows) - 1
        log_ws.update(f"A{start_row}:D{end_row}", append_rows, value_input_option="USER_ENTERED")

        print(f"✅ Vault rotation execution complete. {len(append_rows)} token(s) logged.")

    except APIError as e:
        if "429" in str(e):
            print("❌ Error in vault_rotation_executor: APIError 429 (quota) — skipping this cycle.")
            return
        raise
    except Exception as e:
        print(f"❌ Error in vault_rotation_executor: {e}")
