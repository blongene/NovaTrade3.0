# presale_scorer.py
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import (
    with_sheet_backoff,
    str_or_empty,
    # to_float,  # uncomment if you need it
    ping_webhook_debug,  # ok if not defined; you can remove calls below
)
from nova_heartbeat import log_heartbeat  # ok if present; otherwise comment out

SHEET_URL   = os.getenv("SHEET_URL")
SHEET_NAME  = "Presale_Stream"
SCOPE       = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# ---- Helpers ----

@with_sheet_backoff
def _open_ws():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet(SHEET_NAME)

@with_sheet_backoff
def _get_all_records(ws):
    # Single, cached-ish read: if you’ve added a ws_get_all_records_cached, you could call it here.
    # For portability we use vanilla get_all_records() and let with_sheet_backoff handle retries.
    return ws.get_all_records()

@with_sheet_backoff
def _batch_update(ws, payload):
    # payload = [{"range":"A1", "values":[[...]]}, ...]
    if not payload:
        return
    ws.batch_update(payload, value_input_option="USER_ENTERED")

def run_presale_scorer():
    print("💥 run_presale_scorer() BOOTED")
    try:
        ws = _open_ws()
        rows = _get_all_records(ws)
        print(f"📦 Raw worksheet data length: {len(rows) or 0}")

        if not rows:
            print("⛔️ No presale rows found")
            return

        # Example scoring placeholder:
        # - read once
        # - compute derived fields in-memory
        # - write back only for rows that truly changed
        header = ws.row_values(1)
        hmap = {h: i+1 for i, h in enumerate(header)}

        # ensure output columns exist
        updates_header = False
        for needed in ["Score", "Reviewed", "Last Checked"]:
            if needed not in hmap:
                header.append(needed)
                hmap[needed] = len(header)
                updates_header = True
        if updates_header:
            # Write header once (no duplicated sheet name in A1)
            ws.update("A1", [header])

        batch = []
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Iterate rows (2..N)
        for r_idx, rec in enumerate(rows, start=2):
            token = str_or_empty(rec.get("Token")).upper()
            if not token:
                continue

            # Light heuristic score (edit/extend as needed)
            # Keep extremely cheap to avoid extra API pulls
            decision = str_or_empty(rec.get("Decision")).upper()
            sentiment = str_or_empty(rec.get("Sentiment")).upper()
            score = ""

            if decision in ("IGNORE", "SKIP"):
                score = "0"
            elif "BULL" in sentiment:
                score = "2"
            elif "BEAR" in sentiment:
                score = "-1"
            else:
                score = "1"

            # Only write missing cells (don’t thrash)
            def _col_letter(n: int) -> str:
                s = ""
                while n:
                    n, rem = divmod(n - 1, 26)
                    s = chr(65 + rem) + s
                return s

            # Update Score if blank
            if str_or_empty(rec.get("Score")) == "":
                a1 = f"{_col_letter(hmap['Score'])}{r_idx}"
                batch.append({"range": a1, "values": [[score]]})

            # Mark last-checked on every pass (optional; comment out if too chatty)
            a1_last = f"{_col_letter(hmap['Last Checked'])}{r_idx}"
            batch.append({"range": a1_last, "values": [[now]]})

        if batch:
            _batch_update(ws, batch)
            print(f"✅ Presale scorer wrote {len(batch)} cell(s) (batched).")
        else:
            print("ℹ️ Presale scorer: nothing to update.")

        try:
            log_heartbeat("Presale Scorer", f"Processed {len(rows)} rows")
        except Exception as _e:
            # Non-fatal
            print(f"⚠️ Heartbeat skip (non-fatal): {_e}")

    except Exception as e:
        # This message should all but vanish once the 429 settles; retries are handled by the decorator.
        print(f"💥 FATAL ERROR in presale_scorer: {e}")
        try:
            ping_webhook_debug(f"💥 presale_scorer error: {e}")
        except Exception:
            pass
