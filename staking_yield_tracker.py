# staking_yield_tracker.py
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import (
    with_sheet_backoff,
    str_or_empty,
    to_float,
    ping_webhook_debug,
)
from nova_heartbeat import log_heartbeat

TOKEN = "MIND"               # target staking token
WALLET_BALANCE = 296_139.94  # TODO: wire to wallet monitor later
SHEET_URL = os.getenv("SHEET_URL")
SHEET_NAME = "Rotation_Log"

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def _looks_like_datetime(s: str) -> bool:
    s = str_or_empty(s)
    return ("-" in s and ":" in s) or s.endswith("Z")

def _cell_address(col_idx: int, row_idx: int) -> str:
    n, letters = col_idx, ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row_idx}"

@with_sheet_backoff
def _open_ws():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL).worksheet(SHEET_NAME)

def run_staking_yield_tracker():
    try:
        ws = _open_ws()
        header = ws.row_values(1)
        hmap = {str_or_empty(h): i for i, h in enumerate(header, start=1)}

        token_col = hmap.get("Token")
        claimed_col = hmap.get("Initial Claimed")
        yield_col = hmap.get("Staking Yield (%)") or hmap.get("Staking Yield")
        lastchk_col = hmap.get("Last Checked")

        if token_col is None or claimed_col is None:
            ping_webhook_debug("⚠️ Staking Tracker: missing required columns.")
            return

        # Create optional columns if missing
        updated_header = False
        if yield_col is None:
            header.append("Staking Yield (%)")
            yield_col = len(header)
            updated_header = True
        if lastchk_col is None:
            header.append("Last Checked")
            lastchk_col = len(header)
            updated_header = True
        if updated_header:
            ws.update("A1", [header])

        rows = ws.get_all_records()
        batch, updated_any = [], False
        now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for r_idx, rec in enumerate(rows, start=2):
            token = str_or_empty(rec.get("Token")).upper()
            if token != TOKEN:
                continue

            claimed_raw = rec.get("Initial Claimed")
            if _looks_like_datetime(str_or_empty(claimed_raw)):
                ping_webhook_debug(f"⚠️ Skipping {token} – datetime in Initial Claimed: {claimed_raw}")
                continue

            initial_claimed = to_float(claimed_raw)
            if not initial_claimed:
                ping_webhook_debug(f"⚠️ Skipping {token} – invalid Initial Claimed: {claimed_raw}")
                continue

            yield_pct = round(((WALLET_BALANCE - initial_claimed) / initial_claimed) * 100.0, 4)
            a1_yield = _cell_address(yield_col, r_idx)
            a1_last = _cell_address(lastchk_col, r_idx)
            batch.append({"range": a1_yield, "values": [[f"{yield_pct}%"]]})
            batch.append({"range": a1_last,  "values": [[now_ts]]})

            log_heartbeat("Staking Tracker", f"{token} Yield = {yield_pct}%")
            if yield_pct == 0:
                ping_webhook_debug(f"⚠️ {token} staking yield is 0%. Verify staking is active.")
            updated_any = True

        if batch:
            ws.batch_update(batch, value_input_option="RAW")

        if not updated_any:
            log_heartbeat("Staking Tracker", "Token not found in Rotation_Log")

    except Exception as e:
        ping_webhook_debug(f"❌ Staking Yield Tracker Error: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_staking_yield_tracker()
