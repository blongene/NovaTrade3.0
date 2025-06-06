import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime


def scan_roi_tracking():
    print("\U0001f501 Updating Days Held in Rotation_Log and tracking ROI...")

    # Auth
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    log_ws = sheet.worksheet("Rotation_Log")
    tracking_ws = sheet.worksheet("ROI_Tracking")

    log_data = log_ws.get_all_records()
    existing_rows = tracking_ws.get_all_records()

    now = datetime.utcnow()
    today_str = now.strftime("%Y-%m-%d")

    tracking_updates = []

    for i, row in enumerate(log_data, start=2):
        token = row.get("Token", "").strip()
        timestamp_str = row.get("Timestamp", "").strip()

        if not token or not timestamp_str:
            continue

        try:
            vote_time = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            days_held = (now - vote_time).days
            log_ws.update_cell(i, 9, days_held)  # Update Days Held (Col I)
        except Exception as e:
            print(f"❌ Failed to parse timestamp for {token}: {e}")
            continue

        # ✅ Deduplication check
        if any(r["Token"] == token and r["Date"] == today_str for r in existing_rows):
            continue

        tracking_updates.append([
            token,
            today_str,
            days_held,
            f"{days_held}d since vote"
        ])

    if tracking_updates:
        tracking_ws.append_rows(tracking_updates, value_input_option="USER_ENTERED")
        print(f"✅ ROI Tracker updated {len(tracking_updates)} rows")
    else:
        print("⚠️ No new ROI tracking entries needed (all rows already logged).")
