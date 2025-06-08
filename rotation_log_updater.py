import gspread
import re
import os
from oauth2client.service_account import ServiceAccountCredentials

def run_rotation_log_updater():
    print("🛠 Updating Rotation_Log with ROI_Review_Log data...")

    # Auth
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    log_ws = sheet.worksheet("Rotation_Log")
    review_ws = sheet.worksheet("ROI_Review_Log")

    log_data = log_ws.get_all_values()
    review_data = review_ws.get_all_values()

    log_header = log_data[0]
    review_header = review_data[0]
    log_rows = log_data[1:]
    review_rows = review_data[1:]

    token_idx_log = log_header.index("Token")
    roi_idx_log = log_header.index("Follow-up ROI")
    token_idx_review = review_header.index("Token")
    roi_idx_review = review_header.index("Follow-up ROI")

    review_map = {row[token_idx_review]: row[roi_idx_review] for row in review_rows}

    for i, row in enumerate(log_rows):
        token = row[token_idx_log]
        current_val = row[roi_idx_log].strip()
        new_val = review_map.get(token, "").strip()

        if new_val and re.match(r"^-?\d+(\.\d+)?$", new_val):
            if current_val != new_val:
                log_ws.update_cell(i + 2, roi_idx_log + 1, new_val)
                print(f"✅ Updated {token} ROI → {new_val}")
