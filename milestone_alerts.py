import os
import gspread
from datetime import datetime
import requests

def send_milestone_alert(token, days):
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    bot_token = os.environ.get("BOT_TOKEN")
    if not chat_id or not bot_token:
        print("⚠️ Missing TELEGRAM_CHAT_ID or BOT_TOKEN in environment.")
        return

    message = (
        f"📍 *Milestone Alert: {token}*\n"
        f"– Days Held: {days}d\n"
        f"– This token has now reached a {days}d milestone.\n"
        f"Would you like to review or consider rotation?"
    )

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        r = requests.post(url, json=payload)
        print(f"📬 Milestone alert sent for {token} @ {days}d: {r.text}")
    except Exception as e:
        print(f"⚠️ Telegram error for {token}: {e}")

def run_milestone_alerts():
    try:
        gc = gspread.service_account(filename="sentiment-log-service.json")
        sheet = gc.open_by_url(os.environ["SHEET_URL"])
        ws = sheet.worksheet("Rotation_Stats")
        rows = ws.get_all_records()

        MILESTONES = [3, 7, 30]
        for i, row in enumerate(rows, start=2):  # start=2 for row index
            try:
                token = row["Token"]
                days = int(row["Days Held"])
                if days in MILESTONES:
                    send_milestone_alert(token, days)
            except Exception as e:
                print(f"⚠️ Milestone check error at row {i}: {e}")
    except Exception as outer:
        print(f"❌ Failed to run milestone alerts: {outer}")
