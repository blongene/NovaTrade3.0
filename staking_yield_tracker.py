import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import ping_webhook_debug, log_heartbeat

# === Setup ===
TOKEN = "MIND"
WALLET_BALANCE = 296139.94  # Manually updated, or pulled from wallet monitor in future
SHEET_NAME = "Rotation_Log"
SHEET_URL = os.getenv("SHEET_URL")

def run_staking_yield_tracker():
    try:
        # Auth to Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(SHEET_URL)
        ws = sheet.worksheet(SHEET_NAME)

        data = ws.get_all_records()
        updated = False

        for i, row in enumerate(data, start=2):  # Start at 2 to account for header
            token = row.get("Token", "").strip().upper()
            if token != TOKEN:
                continue

            initial_claimed = float(row.get("Initial Claimed", 0))
            last_balance = WALLET_BALANCE
            yield_percent = round(((last_balance - initial_claimed) / initial_claimed) * 100, 4)
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # Update sheet
            ws.update_acell(f'K{i}', f"{yield_percent}%")  # Staking Yield (%)
            ws.update_acell(f'N{i}', timestamp)            # Last Checked
            log_heartbeat("Staking Tracker", f"{token} Yield = {yield_percent}%")

            # Optional alert if yield is 0
           if yield_percent == 0:
               ping_webhook_debug(f"⚠️ {token} staking yield is 0%. Verify staking is active.")
