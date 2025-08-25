import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
from send_telegram import send_rotation_alert
from utils import get_records_cached

def run_milestone_alerts():
    print("🚀 Running Milestone Alerts...")

    # Auth
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    log_ws = sheet.worksheet("Rotation_Log")
    review_ws = sheet.worksheet("ROI_Review_Log")
    rows = get_records_cached("Some_Tab", ttl_s=180)  # 3‑minute cache

    milestone_days = [3, 7, 14, 30]
    today = datetime.utcnow()
    prompted = 0

    for i, row in enumerate(rows):
        token = row.get("Token", "").strip()
        timestamp_str = row.get("Timestamp", "").strip()
        decision = row.get("Decision", "YES").strip().upper()

        if not token or not timestamp_str:
            continue

        try:
            vote_time = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            days_held = (today - vote_time).days
        except:
            continue

        if days_held in milestone_days:
            print(f"🚀 {token} hit milestone: {days_held}d")
            
            # Check if already exists in ROI_Review_Log
            existing = review_ws.get_all_records()
            if any(r.get("Token", "").strip().upper() == token.upper() for r in existing):
                continue

            # Append new review row
            review_ws.append_row([token, today.strftime("%Y-%m-%d"), "", "", ""], value_input_option="USER_ENTERED")

            # Send Telegram rotation prompt
            message = f"🔁 *{token}* has reached a {days_held} day milestone.\nWould you still vote YES knowing what you know now?"
            send_rotation_alert(token, message)
            prompted += 1

    print(f"✅ Milestone alert check complete. {prompted} Telegrams sent.")
