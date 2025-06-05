from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# Updated ROI Tracker to avoid writing '2d since vote' into Rotation_Log
def scan_roi_tracking():
    print("🔁 Updating Days Held in Rotation_Log and tracking ROI...")

    # Load worksheet and data
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    log_ws = sheet.worksheet("Rotation_Log")
    tracking_ws = sheet.worksheet("ROI_Tracking")

    log_data = log_ws.get_all_records()
    now = datetime.utcnow()

    tracking_updates = []

    for i, row in enumerate(log_data, start=2):  # Row offset for header
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

        # Write to ROI_Tracking instead of Rotation_Log!Follow-up ROI
        tracking_updates.append([token, now.strftime("%Y-%m-%d"), days_held, f"{days_held}d since vote"])

    if tracking_updates:
        tracking_ws.append_rows(tracking_updates, value_input_option="USER_ENTERED")
        print(f"✅ ROI Tracker updated {len(tracking_updates)} rows")
