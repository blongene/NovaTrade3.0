
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

def run_roi_feedback_sync():
    print("🔄 Syncing ROI feedback to ROI_Review_Log...")

    # Auth
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    feedback_ws = sheet.worksheet("Rotation_Log")
    review_ws = sheet.worksheet("ROI_Review_Log")

    feedback_data = feedback_ws.get_all_records()
    headers = feedback_ws.row_values(1)

    # Identify column indexes
    try:
        token_idx = headers.index("Token")
        roi_idx = headers.index("Follow-up ROI")
        vote_idx = headers.index("Would You Say YES Again?")
        synced_idx = headers.index("Synced?")
    except ValueError as e:
        print("❌ Missing required column in ROI_Tracking:", e)
        return

    new_logs = []
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for i, row in enumerate(feedback_data):
        synced = row.get("Synced?", "").strip().upper()
        token = row.get("Token", "").strip()
        roi = row.get("Follow-up ROI", "").strip()
        vote = row.get("Would You Say YES Again?", "").strip()

        if synced in ["✅", "YES", "TRUE"] or not token or not roi:
            continue  # Skip already synced or incomplete rows

        # Prepare new log entry
        log_entry = [now, token, roi, vote]
        new_logs.append(log_entry)

        # Mark as synced
        feedback_ws.update_cell(i + 2, synced_idx + 1, "✅")

    if new_logs:
        print(f"✍️ Logging {len(new_logs)} new feedback entries...")
        review_ws.append_rows(new_logs)
    else:
        print("🟡 No new ROI feedback to sync.")
