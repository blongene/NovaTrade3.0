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
    now = datetime.utcnow()

    tracking_updates = []

    for i, row in enumerate(log_data, start=2):  # Start at row 2
        token = row.get("Token", "").strip()
        timestamp_str = row.get("Timestamp", "").strip()

        if not token or not timestamp_str:
            continue

        try:
            vote_time = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            days_held = (now - vote_time).days
            log_ws.update_cell(i, 9, days_held)  # Column I = Days Held
        except Exception as e:
            print(f"❌ Failed to parse timestamp for {token}: {e}")
            continue

        # Only write to ROI_Tracking — not Follow-up ROI in Rotation_Log
        tracking_updates.append([
            token,
            now.strftime("%Y-%m-%d"),
            days_held,
            f"{days_held}d since vote"
        ])

    if tracking_updates:
        tracking_ws.append_rows(tracking_updates, value_input_option="USER_ENTERED")
        print(f"✅ ROI Tracker updated {len(tracking_updates)} rows")
