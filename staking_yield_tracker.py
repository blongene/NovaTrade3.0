import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import ping_webhook_debug
from nova_heartbeat import log_heartbeat

# === Setup ===
TOKEN = "MIND"
WALLET_BALANCE = 296139.94
SHEET_NAME = "Rotation_Log"
SHEET_URL = os.getenv("SHEET_URL")

def run_staking_yield_tracker():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        ws = client.open_by_url(SHEET_URL).worksheet(SHEET_NAME)
        data = ws.get_all_records()

        updated = False
        for i, row in enumerate(data, start=2):  # row offset
            token = str(row.get("Token", "")).strip().upper()
            if token != TOKEN:
                continue

            val = row.get("Initial Claimed", "")
            # Guard: skip datetime-like strings
            if isinstance(val, str) and "-" in val and ":" in val:
                print(f"⚠️ Skipping {token} – looks like datetime: {val}")
                ping_webhook_debug(f"⚠️ Skipping {token} – looks like datetime: {val}")
                continue

            try:
                initial_claimed = float(str(val).replace("%", "").strip())
            except Exception:
                print(f"⚠️ Skipping {token} – invalid Initial Claimed value: {val}")
                ping_webhook_debug(f"⚠️ Skipping {token} – invalid Initial Claimed value: {val}")
                continue

            last_balance = WALLET_BALANCE
            yield_percent = round(((last_balance - initial_claimed) / initial_claimed) * 100, 4)
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            ws.update_acell(f'K{i}', f"{yield_percent}%")
            ws.update_acell(f'N{i}', timestamp)

            log_heartbeat("Staking Tracker", f"{token} Yield = {yield_percent}%")
            if yield_percent == 0:
                ping_webhook_debug(f"⚠️ {token} staking yield is 0%. Verify staking is active.")
            updated = True

        if not updated:
            log_heartbeat("Staking Tracker", "Token not found in Rotation_Log")

    except Exception as e:
        ping_webhook_debug(f"❌ Staking Yield Tracker Error: {str(e)}")
        print(f"❌ Error: {e}")
