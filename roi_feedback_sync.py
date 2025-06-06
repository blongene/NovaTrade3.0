import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

def run_roi_feedback_sync():
    print("🔄 Syncing ROI feedback from ROI_Review_Log...")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    review_ws = sheet.worksheet("ROI_Review_Log")
    stats_ws = sheet.worksheet("Rotation_Stats")

    review_data = review_ws.get_all_records()
    stats_data = stats_ws.get_all_records()
    review_headers = review_ws.row_values(1)
    stats_headers = stats_ws.row_values(1)

    try:
        token_idx = review_headers.index("Token")
        vote_idx = review_headers.index("Would You Say YES Again?")
        feedback_idx = review_headers.index("Feedback")
        synced_idx = review_headers.index("Synced?")
    except ValueError as e:
        print("❌ Missing column in ROI_Review_Log:", e)
        return

    try:
        stats_token_idx = stats_headers.index("Token")
        revote_col = stats_headers.index("Re-Vote") + 1
        notes_col = stats_headers.index("Feedback Notes") + 1
    except ValueError as e:
        print("❌ Missing column in Rotation_Stats:", e)
        return

    updated = 0
    for i, row in enumerate(review_data):
        token = row.get("Token", "").strip().upper()
        vote = row.get("Would You Say YES Again?", "").strip().upper()
        feedback = row.get("Feedback", "").strip()
        synced = row.get("Synced?", "").strip()

        if synced in ["✅", "TRUE", "YES"] or not token or not vote:
            continue

        for j, stat in enumerate(stats_data):
            if stat.get("Token", "").strip().upper() == token:
                stats_ws.update_cell(j + 2, revote_col, vote)
                if feedback:
                    stats_ws.update_cell(j + 2, notes_col, feedback)
                review_ws.update_cell(i + 2, synced_idx + 1, "✅")
                updated += 1
                print(f"📥 Synced feedback for {token}: {vote}, Notes: {feedback}")
                break

    print(f"✅ ROI feedback sync complete. {updated} row(s) updated.")
