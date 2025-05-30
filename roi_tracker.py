import gspread
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.environ.get("SHEET_URL")

def scan_roi_tracking():
    print("📈 Checking for ROI milestone follow-ups...")

    # Authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(SHEET_URL)
    log_ws = sheet.worksheet("Rotation_Log")

    # Load data
    data = log_ws.get_all_values()
    headers = data[0]
    rows = data[1:]

    # Required columns
    try:
        timestamp_idx = headers.index("Timestamp")
        days_held_idx = headers.index("Days Held")
        followup_idx = headers.index("Follow-up ROI")
    except ValueError:
        print("❌ Missing required columns: 'Timestamp', 'Days Held', or 'Follow-up ROI'")
        return

    now = datetime.utcnow()

    for i, row in enumerate(rows):
        if len(row) <= max(timestamp_idx, days_held_idx, followup_idx):
            continue

        try:
            timestamp_str = row[timestamp_idx].strip()
            timestamp_dt = datetime.strptime(timestamp_str, "%m/%d/%Y %H:%M:%S")
            days_held = (now - timestamp_dt).days

            # For now, follow-up ROI is time-based: "Xd since vote"
            roi_text = f"{days_held}d since vote"

            # Update both columns in Rotation_Log
            log_ws.update_cell(i + 2, days_held_idx + 1, str(days_held))
            log_ws.update_cell(i + 2, followup_idx + 1, roi_text)

            print(f"🔁 Updated ROI tracker for row {i+2} — {roi_text}")
        except Exception as e:
            print(f"⚠️ Failed to update row {i+2}: {e}")
