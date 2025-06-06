# ✅ Updated rotation_signal_engine.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
from send_telegram import send_rotation_alert

PROMPT_MEMORY = {}

MILESTONES = [3, 7, 14, 30]

# rotation_signal_engine.py

def scan_rotation_candidates():
    print("🧠 scan_rotation_candidates stub is active.")

def run_milestone_alerts():
    print("\U0001F6A7 Scanning for milestone alerts...")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    log_ws = sheet.worksheet("Rotation_Log")
    review_ws = sheet.worksheet("ROI_Review_Log")

    log_data = log_ws.get_all_records()
    review_data = review_ws.get_all_records()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.utcnow().date()

    for row in log_data:
        token = row.get("Token", "").strip()
        timestamp = row.get("Timestamp", "").strip()
        days_held = int(row.get("Days Held", 0))
        roi = row.get("Follow-up ROI", "")

        if not token or not timestamp or not roi:
            continue

        if days_held in MILESTONES and token not in PROMPT_MEMORY:
            print(f"\u2728 Milestone hit: {token} — {days_held}d")

            # Check if already exists in review log
            if any(r["Token"].strip() == token and int(r.get("Days Held", 0)) == days_held for r in review_data):
                continue

            # Add to ROI_Review_Log
            new_row = [
                now,
                token,
                days_held,
                roi,
                "",  # Final ROI
                "",  # Re-Vote
                "",  # Feedback
                "",  # Synced?
                ""   # Would You Say YES Again?
            ]
            review_ws.append_row(new_row, value_input_option="USER_ENTERED")

            # Send Telegram alert
            msg = f"⏳ *ROI Milestone Reached: {token}*\n– Days Held: {days_held}\n– ROI: {roi}%\n\nWould you still vote YES today?"
            send_rotation_alert(token, msg)
            PROMPT_MEMORY[token] = True

    print("✅ Milestone alert scan complete.")
