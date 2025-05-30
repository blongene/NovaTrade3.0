import gspread
from datetime import datetime
from utils import send_telegram_message
from oauth2client.service_account import ServiceAccountCredentials
import os

def run_milestone_alerts():
    print("🚀 Checking for milestone ROI alerts...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    ws = sheet.worksheet("Rotation_Log")

    rows = ws.get_all_records()
    for i, row in enumerate(rows):
        try:
            token = row.get("Token", "")
            days = int(row.get("Days Held", 0))
            decision = row.get("Decision", "").strip().upper()

            if decision != "YES":
                continue

            if days in [3, 7, 14, 30]:
                msg = (
                    f"📍 *Milestone Alert: {token}*\n"
                    f"– Days Held: {days}d\n"
                    f"This token has now reached a {days}d milestone.\n"
                    f"Would you like to review or consider rotation?"
                )
                send_telegram_message(msg)
                print(f"📬 Milestone alert sent for {token} @ {days}d")
        except Exception as e:
            print(f"❌ Milestone Alert Engine failed for row {i+2}: {e}")
