import gspread
import re
import os
from oauth2client.service_account import ServiceAccountCredentials

def run_rotation_log_cleanup():
    print("🧹 Running cleanup on Rotation_Log...")

    # Authenticate and open sheet
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    log_ws = sheet.worksheet("Rotation_Log")

    # Get data
    log_data = log_ws.get_all_values()
    header = log_data[0]
    data = log_data[1:]

    roi_col = header.index("Follow-up ROI") + 1  # 1-based index for update_cell

    for i, row in enumerate(data):
        value = row[roi_col - 1].strip()
        if value and not re.match(r"^-?\d+(\.\d+)?$", value):
            log_ws.update_cell(i + 2, roi_col, "")  # +2 = 1 for header, 1 for 1-based indexing
            print(f"❌ Non-numeric ROI cleared in row {i+2}: '{value}'")

    print("✅ Cleanup complete. Rotation_Log now sanitized.")
