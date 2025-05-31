import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

def run_roi_feedback_sync():
    print("🔄 Syncing ROI feedback from ROI_Review_Log...")

    # Authenticate with Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    # Open the correct sheet and worksheet
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    review_ws = sheet.worksheet("ROI_Review_Log")

    # Load worksheet data
    data = review_ws.get_all_records()
    headers = review_ws.row_values(1)

    try:
        timestamp_idx = headers.index("Timestamp")
        token_idx = headers.index("Token")
        roi_idx = headers.index("ROI")
        vote_idx = headers.index("Would You Say YES Again?")
        synced_idx = headers.index("Synced?")
    except ValueError as e:
        print("❌ Missing required column in ROI_Review_Log:", e)
        return

    new_syncs = []
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for i, row in enumerate(data):
        synced = str(row.get("Synced?", "")).strip().upper()
        token = str(row.get("Token", "")).strip()
        roi = str(row.get("ROI", "")).strip()
        vote = str(row.get("Would You Say YES Again?", "")).strip()

        if synced in ["✅", "YES", "TRUE"] or not token or not roi:
            continue  # Skip already synced or incomplete

        # This is where you'd handle storing or using the feedback (e.g. write to a DB or another tab)
        print(f"📥 Feedback received on {token} — ROI: {roi}, Vote: {vote}")

        # Mark as synced in the sheet
        review_ws.update_cell(i + 2, synced_idx + 1, "✅")

    print("✅ ROI feedback sync complete.")
