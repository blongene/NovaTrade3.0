import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message
import os

def run_milestone_alerts():
    print("🚀 Checking for milestone ROI alerts...")

    # Authenticate Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    ws = sheet.worksheet("Rotation_Log")

    # Fetch environment values using the correct Render keys
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    bot_token = os.getenv("BOT_TOKEN")

    if not chat_id or not bot_token:
        print("⚠️ Missing TELEGRAM_CHAT_ID or BOT_TOKEN in environment.")
        return

    rows = ws.get_all_records()

    for i, row in enumerate(rows):
        try:
            token = row.get("Token", "")
            days = int(row.get("Days Held", 0))
            decision = row.get("Decision", "").strip().upper()

            if decision != "YES":
                continue

            if days in [3, 7, 14, 30]:
                message = (
                    f"📍 *Milestone Alert: {token}*\n"
                    f"– Days Held: {days}d\n"
                    f"This token has now reached a {days}d milestone.\n"
                    f"Would you like to review or consider rotation?"
                )
                send_telegram_message(message)
                print(f"📬 Milestone alert sent for {token} @ {days}d")

        except Exception as e:
            print(f"❌ Milestone Alert Engine failed for row {i+2}: {e}")
