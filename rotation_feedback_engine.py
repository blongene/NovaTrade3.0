import os
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

PROMPT_MEMORY = {}

def run_rotation_feedback_engine():
    try:
        # Authenticate
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        stats_ws = sheet.worksheet("Rotation_Stats")
        review_ws = sheet.worksheet("ROI_Review_Log")

        stats_rows = stats_ws.get_all_records()
        review_rows = review_ws.get_all_records()

        for i, row in enumerate(stats_rows, start=2):
            token = row.get("Token", "").strip()
            decision = row.get("Decision", "").strip().upper()
            roi = str(row.get("Follow-up ROI", "")).strip()

            try:
                days_held = int(row.get("Days Held", 0))
            except ValueError:
                continue

            if decision != "YES" or days_held not in [7, 14, 30]:
                continue

            memory_key = f"{token}_review_{days_held}"
            if PROMPT_MEMORY.get(memory_key):
                continue

            # Check if already answered in ROI_Review_Log
            already_answered = any(
                r["Token"].strip().upper() == token.upper()
                and int(r.get("Days Held", 0)) == days_held
                and r.get("Would You Say YES Again?", "").strip() != ""
                for r in review_rows
            )
            if already_answered:
                continue

            # 🔁 Prompt user via Telegram
            message = f"📊 *Feedback Request: {token}*\n– Days Held: {days_held}d\n– ROI: {roi}\n\nWould you vote YES again?"
            keyboard = {
                "inline_keyboard": [[
                    {"text": "✅ YES Again", "callback_data": f"REYES|{token}|{days_held}"},
                    {"text": "❌ NO", "callback_data": f"RENO|{token}|{days_held}"}
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
                print(f"⚠️ Telegram ping failed for {token}: {resp.status_code} - {resp.text}")

    except Exception as e:
        print(f"❌ rotation_feedback_engine error: {e}")
