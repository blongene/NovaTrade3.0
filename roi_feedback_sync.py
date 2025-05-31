import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def sync_roi_feedback():
    print("🔄 Syncing ROI review feedback...")

    # Auth and Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    review_ws = sheet.worksheet("ROI_Review_Log")
    stats_ws = sheet.worksheet("Rotation_Stats")

    # Load values
    review_data = review_ws.get_all_values()
    stats_data = stats_ws.get_all_values()
    review_headers = review_data[0]
    stats_headers = stats_data[0]
    review_rows = review_data[1:]
    stats_rows = stats_data[1:]

    token_idx = review_headers.index("Token")
    feedback_idx = review_headers.index("Would You Say YES Again?")
    
    if "Reaffirmed" not in stats_headers:
        stats_ws.update_cell(1, len(stats_headers)+1, "Reaffirmed")
        stats_headers.append("Reaffirmed")

    reaffirm_idx = stats_headers.index("Reaffirmed")
    stats_token_idx = stats_headers.index("Token")

    updates_made = 0

    for i, row in enumerate(review_rows):
        if len(row) <= feedback_idx or row[feedback_idx].strip() == "":
            continue

        token = row[token_idx].strip().upper()
        feedback = row[feedback_idx].strip().upper()

        for j, stats_row in enumerate(stats_rows):
            if len(stats_row) > stats_token_idx and stats_row[stats_token_idx].strip().upper() == token:
                stats_ws.update_cell(j+2, reaffirm_idx+1, feedback)
                print(f"✅ Synced feedback for {token}: {feedback}")
                updates_made += 1
                break

        # Reset user feedback cell so it doesn't resync
        review_ws.update_cell(i+2, feedback_idx+1, "")

    if updates_made == 0:
        print("ℹ️ No new feedback to sync.")
    else:
        print(f"🔁 Synced {updates_made} feedback responses to Rotation_Stats.")
