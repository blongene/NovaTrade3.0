import gspread
import re
import os
from oauth2client.service_account import ServiceAccountCredentials

def run_rotation_log_cleanup():
    print("🧹 Running cleanup on Rotation_Log...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        log_ws = sheet.worksheet("Rotation_Log")

        log_data = log_ws.get_all_values()
        if not log_data:
            print("⚠️ No data found in Rotation_Log.")
            return

        header = log_data[0]
        data = log_data[1:]

        if "Follow-up ROI" not in header:
            print("⚠️ 'Follow-up ROI' column missing in Rotation_Log.")
            return

        roi_col = header.index("Follow-up ROI") + 1
        cleaned = 0

        for i, row in enumerate(data):
            if len(row) <= roi_col - 1:
                continue
            value = row[roi_col - 1].strip()
            if value and not re.match(r"^-?\d+(\.\d+)?$", value):
                log_ws.update_cell(i + 2, roi_col, "")
                cleaned += 1
                print(f"❌ Cleared non-numeric ROI in row {i+2}: '{value}'")

        print(f"✅ Cleanup complete. {cleaned} entries sanitized.")

    except Exception as e:
        print(f"❌ Rotation Log cleanup error: {e}")
