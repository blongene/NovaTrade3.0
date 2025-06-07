import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_rotation_log_updater():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    roi_ws = sheet.worksheet("ROI_Tracking")
    log_ws = sheet.worksheet("Rotation_Log")

    roi_rows = roi_ws.get_all_records()
    log_rows = log_ws.get_all_records()

    updated = 0
    for i, row in enumerate(log_rows, start=2):
        token = row.get("Token", "").strip()
        roi_entry = next((r for r in roi_rows if r["Token"] == token), None)
        if not roi_entry:
            continue

        roi_value = roi_entry.get("ROI", "")
        try:
            float_val = float(roi_value)
            log_ws.update_cell(i, 9, float_val)
            updated += 1
        except:
            continue

    print(f"✅ Follow-up ROI patch complete. {updated} cell(s) updated.")
