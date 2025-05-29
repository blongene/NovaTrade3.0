import gspread
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import requests

# Setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("token_vault.json", scope)
sheet = gspread.authorize(creds).open_by_url("https://docs.google.com/spreadsheets/d/1rE6rbUnCPiL8OgBj6hPWNppOV1uaII8im41nrv-x1xg/edit")

# Telegram Secrets
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_rotation_alert(token, roi, sentiment, days_held):
    message = (
        f"🔁 *Rotation Suggestion: {token}*\n"
        f"- Days Held: {days_held}\n"
        f"- ROI: {roi}x\n"
        f"- Sentiment: {sentiment}\n\n"
        f"Would you like to rotate out of this token?"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    requests.post(url, data=data)

def scan_rotation_candidates():
    print("🧠 Running Rotation Signal Engine...")
    try:
        ws = sheet.worksheet("Rotation_Stats")
        data = ws.get_all_records()
        print(f"📊 Retrieved {len(data)} rows from Rotation_Stats")
    except Exception as e:
        print(f"❌ Sheet error: {e}")
        return

    for row in data:
        print(f"🔎 Scanning row: {row}")
        try:
            token = row.get("Token", "")
            status = row.get("Status", "")
            days_held = int(row.get("Days Held", 0))
            roi = float(row.get("Follow-up ROI", 0))
            sentiment = row.get("Sentiment", "").strip().lower()

            if status == "Active" and days_held >= 2:
                if roi <= 0.5 or sentiment == "weak":
                    print(f"⚠️ Triggering alert for {token}")
                    send_rotation_alert(token, roi, sentiment, days_held)
        except Exception as e:
            print(f"❌ Error processing row {row}: {e}")



