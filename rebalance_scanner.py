import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime
from send_telegram import send_rotation_alert


def run_rebalance_scanner():
    print("\U0001f4ca Running Rebalance Scanner...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        target_ws = sheet.worksheet("Portfolio_Targets")
        rows = target_ws.get_all_records()

        for row in rows:
            token = row.get("Token", "").strip()
            status = row.get("Notes", "").strip().lower()

            if status in ["overweight", "underdsized"]:
                message = f"⚖️ *Rebalance Candidate Detected: {token}*\n– Status: {status.title()}\n\nWould you like to adjust this holding to meet target allocations?"
                send_rotation_alert(token, message)

        print("✅ Rebalance scanner complete.")

    except Exception as e:
        print(f"❌ rebalance_scanner failed: {e}")
