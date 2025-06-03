import os
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

PROMPT_MEMORY = {}

def run_rotation_feedback_engine():
    try:
        # Setup Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")
        review_ws = sheet.worksheet("ROI_Review_Log")

        rows = stats_ws.get_all_records()
        for row in rows:
            token = row.get("Token", "").strip()
            decision = row.get("Decision", "").strip().upper()
            days_held = int(row.get("Days Held", "0") or 0)
            roi = row.get("Follow-up ROI", "")
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            if decision != "YES" or days_held not in [7, 14, 30]:
                continue

            memory_key = f"{token}_review_{days_held}"
            if PROMPT_MEMORY.get(memory_key):
                continue

            # Send Telegram ping
            message = f"📊 *Feedback Request: {token}*\n– Days Held: {days_held}d\n– ROI: {roi}\n\nWould you vote YES again?"
            keyboard = {
                "inline_keyboard": [[
                    {"text": "✅ YES Again", "callback_data": f"REYES|{token}|{days_held}|{roi}"},
                    {"text": "❌ NO", "callback_data": f"RENO|{token}|{days_held}|{roi}"}
                ]]
            }

            resp = requests.post(
                f"https://api.telegram.org/bot{os.getenv('BOT_TOKEN')}/sendMessage",
                json={
                    "chat_id": os.getenv("CHAT_ID"),
                    "text": message,
                    "parse_mode": "Markdown",
                    "reply_markup": keyboard
                }
            )

            if resp.ok:
                PROMPT_MEMORY[memory_key] = True
                print(f"📬 Feedback ping sent for {token} @ {days_held}d")
            else:
                print(f"⚠️ Telegram ping failed for {token}: {resp.text}")

    except Exception as e:
        print(f"❌ rotation_feedback_engine error: {e}")
