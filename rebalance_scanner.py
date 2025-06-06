import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime
from send_telegram import send_rotation_alert

def run_rebalance_scanner():
    print("📊 Running Rebalance Scanner...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Portfolio_Targets")

        data = ws.get_all_records()
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        pinged = 0

        for i, row in enumerate(data, start=2):
            token = str(row.get("Token", "")).strip()
            status = str(row.get("Notes", "")).lower()
            last_sync = str(row.get("Last Sync", "")).strip()

            if not token or status not in ["overweight", "undersized"]:
                continue

            # Skip if ping already sent today
            if last_sync.startswith(str(datetime.utcnow().date())):
                continue

            message = (
                f"⚖️ *Rebalance Opportunity: {token}*\n"
                f"– Current Status: {status.title()}\n"
                f"– Portfolio %: {row.get('Current %', '?')}%\n"
                f"– Target %: {row.get('Target %', '?')}%\n\n"
                f"Would you like to rebalance this asset?"
            )
            send_rotation_alert(token, message)
            ws.update_cell(i, ws.find("Last Sync").col, now)
            pinged += 1

        print(f"✅ Rebalance scanner complete. {pinged} alert(s) sent.")

    except Exception as e:
        print(f"❌ rebalance_scanner failed: {e}")
