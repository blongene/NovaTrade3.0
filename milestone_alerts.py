import os
import gspread
from datetime import datetime
from utils import send_telegram_message


PROMPT_MEMORY = {}

def run_milestone_alerts():
    try:
        gc = gspread.service_account(filename="sentiment-log-service.json")
        sh = gc.open_by_url(os.getenv("SHEET_URL"))
        ws = sh.worksheet("Rotation_Stats")
        rows = ws.get_all_records()

        milestone_days = [3, 7, 14, 30]
        for i, row in enumerate(rows, start=2):
            token = str(row.get("Token", "")).strip()
            decision = str(row.get("Decision", "")).strip().upper()
            days_held_raw = row.get("Days Held", "")

            try:
                days_held = int(str(days_held_raw).strip())
            except:
                continue

            if decision != "YES" or not token:
                continue

            if days_held in milestone_days:
                milestone_key = f"{token}_milestone_{days_held}"
                if PROMPT_MEMORY.get(milestone_key):
                    continue  # Already alerted

                message = (
                    f"📍 *Milestone Alert: {token}*\n"
                    f"– Days Held: {days_held}d\n"
                    f"This token has now reached a {days_held}d milestone.\n"
                    f"Would you like to review or consider rotation?"
                )
                sent = send_telegram_message(message)

                if sent:
                    PROMPT_MEMORY[milestone_key] = True
                    print(f"📬 Milestone alert sent for {token} @ {days_held}d: {sent}")
                else:
                    print(f"⚠️ Failed to send milestone alert for {token}")

    except Exception as e:
        print(f"❌ Milestone Alert Engine failed: {e}")
