# unlock_horizon_alerts.py

import os
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message, ping_webhook_debug


def run_unlock_horizon_alerts():
    print("🔍 Checking for upcoming unlocks...")
    
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Claim_Tracker")

        data = ws.get_all_records()
        today = datetime.utcnow().date()
        horizon = today + timedelta(days=1)

        headers = ws.row_values(1)
        token_col = headers.index("Token") + 1
        claimed_col = headers.index("Claimed?") + 1
        unlock_col = headers.index("Unlock Date") + 1
        last_alerted_col = headers.index("Last Alerted") + 1

        alerts_sent = 0

        for i, row in enumerate(data, start=2):  # row 2 is the first data row
            token = row.get("Token", "").strip().upper()
            claimed = row.get("Claimed?", "").strip().lower()
            unlock_str = row.get("Unlock Date", "").strip()
            last_alerted = row.get("Last Alerted", "").strip()

            if claimed == "claimed" or not unlock_str:
                continue

            try:
                unlock_date = datetime.strptime(unlock_str, "%Y-%m-%d").date()
            except ValueError:
                print(f"⚠️ Invalid date format for {token}: {unlock_str}")
                continue

            if unlock_date == horizon and last_alerted != today.isoformat():
                msg = f"🗓 *Upcoming Unlock Alert:*

$*{token}* is scheduled to unlock **tomorrow** ({unlock_date}).\nPrepare to monitor claim window or stake/rotate accordingly."
                send_telegram_message(msg)
                ws.update_cell(i, last_alerted_col, today.isoformat())
                alerts_sent += 1

        print(f"✅ Unlock horizon check complete. {alerts_sent} alert(s) sent.")
        ping_webhook_debug(f"🔹 Horizon Alerts: {alerts_sent} sent")

    except Exception as e:
        print(f"❌ Error in run_unlock_horizon_alerts: {e}")
        ping_webhook_debug(f"❌ Unlock horizon alert error: {e}")
