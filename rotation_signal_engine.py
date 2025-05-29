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

print(f"⚠️ Triggering alert for {row['Token']}")
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
    ws = sheet.worksheet("Rotation_Stats")
    data = ws.get_all_records()
    for row in data:
        try:
            print(f"Checking {row['Token']}: ROI {row['Follow-up ROI']}, Sentiment {row['Sentiment']}, Days Held {row['Days Held']}")
            if row["Status"] == "Active" and int(row["Days Held"]) >= 2:
                roi = float(row["Follow-up ROI"])
                sentiment = row["Sentiment"]
                if roi <= 0.5 or sentiment.strip().lower() == "weak":
                    print(f"🚨 Triggering rotation alert for {row['Token']}")
                    send_rotation_alert(row["Token"], roi, sentiment, row["Days Held"])
        except Exception as e:
            print(f"Error processing row {row}: {e}")

