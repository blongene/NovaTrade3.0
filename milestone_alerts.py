import os
import gspread
import requests
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.environ.get("SHEET_URL")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

MILESTONES = [3, 7, 30]  # Days held milestones to trigger alerts

def run_milestone_alerts():
    print("🚀 Checking for milestone ROI alerts...")

    # Authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(SHEET_URL)
    log_ws = sheet.worksheet("Rotation_Log")

    data = log_ws.get_all_values()
    headers = data[0]
    rows = data[1:]

    try:
        token_idx = headers.index("Token")
        days_idx = headers.index("Days Held")
        last_col = len(headers) + 1  # Extra column for "Milestone Alerted" memory (optional)

        for i, row in enumerate(rows):
            if len(row) <= max(token_idx, days_idx):
                continue

            token = row[token_idx].strip().upper()
            try:
                days = int(row[days_idx])
            except ValueError:
                continue

            # Check if any milestone matches this row
            if days in MILESTONES:
                message = f"""📍 *Milestone Alert: {token}*
- Days Held: {days}
- This token has now reached a {days}d hold milestone.

Would you like to review or consider rotation?"""

                payload = {
                    "chat_id": CHAT_ID,
                    "text": message,
                    "parse_mode": "Markdown"
                }

                try:
                    response = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload)
                    if response.ok:
                        print(f"📬 Milestone alert sent for {token} @ {days}d")
                    else:
                        print(f"⚠️ Telegram error for {token}: {response.text}")
                except Exception as e:
                    print(f"❌ Failed to send milestone alert for {token}: {e}")

    except Exception as e:
        print(f"❌ Milestone Alert Engine failed: {e}")
