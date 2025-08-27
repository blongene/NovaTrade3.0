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

# === Config ===
TOKEN = "MIND"  # target staking token
WALLET_BALANCE = 296_139.94  # TODO: wire to wallet monitor later
SHEET_URL = os.getenv("SHEET_URL")
SHEET_NAME = "Rotation_Log"

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]


def _looks_like_datetime(s: str) -> bool:
    """Heuristic: treats common ISO-ish strings as datetime (not numeric %)."""
    s = str_or_empty(s)
    return ("-" in s and ":" in s) or s.endswith("Z")


def _cell_address(col_idx: int, row_idx: int) -> str:
    n = col_idx
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row_idx}"


@with_sheet_backoff
def _open_ws():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet(SHEET_NAME)


def run_staking_yield_tracker():
    try:
        ws = _open_ws()

        # Header map
        header = ws.row_values(1)
        hmap = {str_or_empty(h): i for i, h in enumerate(header, start=1)}

        # Required columns (by name)
        token_col = hmap.get("Token")
        claimed_col = hmap.get("Initial Claimed")
        yield_col = hmap.get("Staking Yield (%)") or hmap.get("Staking Yield %") or hmap.get("Staking Yield")
        lastchk_col = hmap.get("Last Checked")

        if token_col is None or claimed_col is None:
            ping_webhook_debug("⚠️ Staking Tracker: missing required columns (Token / Initial Claimed).")
            return

        # Fallback: create optional columns if missing
        updates_header = False
        if yield_col is None:
            header.append("Staking Yield (%)")
            yield_col = len(header)
            updates_header = True
        if lastchk_col is None:
            header.append("Last Checked")
            lastchk_col = len(header)
            updates_header = True
        if updates_header:
            ws.update("A1", [header])

        # Read all rows once
        rows = ws.get_all_records()
        batch = []
        updated_any = False
        now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for r_idx, rec in enumerate(rows, start=2):
            token = str_or_empty(rec.get("Token")).upper()
            if token != TOKEN:
                continue

            claimed_raw = rec.get("Initial Claimed")
            # Guard: if looks like a datetime, skip numeric yield calc
            if _looks_like_datetime(str_or_empty(claimed_raw)):
                msg = f"⚠️ Skipping {token} – Initial Claimed looks like a datetime: {claimed_raw}"
                print(msg)
                ping_webhook_debug(msg)
                continue

            initial_claimed = to_float(claimed_raw)
            if initial_claimed is None or initial_claimed <= 0:
                msg = f"⚠️ Skipping {token} – invalid Initial Claimed value: {claimed_raw}"
                print(msg)
                ping_webhook_debug(msg)
                continue

            # Compute yield %
            last_balance = WALLET_BALANCE
            yield_pct = round(((last_balance - initial_claimed) / initial_claimed) * 100.0, 4)

            # Prepare two single-cell updates (batched)
            a1_yield = _cell_address(yield_col, r_idx)
            a1_last = _cell_address(lastchk_col, r_idx)
            batch.append({"range": f"{SHEET_NAME}!{a1_yield}", "values": [[f"{yield_pct}%"]]})
            batch.append({"range": f"{SHEET_NAME}!{a1_last}", "values": [[now_ts]]})

            log_heartbeat("Staking Tracker", f"{token} Yield = {yield_pct}%")
            if yield_pct == 0:
                ping_webhook_debug(f"⚠️ {token} staking
