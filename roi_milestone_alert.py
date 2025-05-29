import gspread
import os
import requests
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

# Google Sheets setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("token_vault.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1rE6rbUnCPiL8OgBj6hPWNppOV1uaII8im41nrv-x1xg/edit")
ws = sheet.worksheet("ROI_Tracking")

# Telegram setup
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_roi_alert(token, milestone, current_roi):
    message = (
        f"📈 *ROI Milestone Alert!*\n"
        f"Token: {token}\n"
        f"Milestone: {milestone}\n"
        f"Current ROI: {current_roi}x\n"
        f"Would you like to take action?"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    requests.post(url, data=data)

def check_roi_milestones():
    today_str = datetime.now().strftime("%-m/%-d/%Y").replace('/0', '/')
    rows = ws.get_all_records()
    print(f"🔍 Checking {len(rows)} ROI milestone rows...")
    for row in rows:
        try:
            if row["Target Date"].strip() == today_str:
                token = row["Token"]
                milestone = row["Milestone"]
                current_roi = row.get("ROI at Check", "N/A")
                print(f"🚨 Sending ROI alert for {token} ({milestone})")
                send_roi_alert(token, milestone, current_roi)
        except Exception as e:
            print(f"Error processing row: {e}")

if __name__ == "__main__":
    check_roi_milestones()
