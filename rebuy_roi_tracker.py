# rebuy_roi_tracker.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_rebuy_roi_tracker():
    print("🔁 Syncing Rebuy ROI → Rotation_Stats...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        log_data = log_ws.get_all_records()
        stats_data = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        # Column indexes
        rebuy_roi_col = headers.index("Rebuy ROI") + 1 if "Rebuy ROI" in headers else len(headers) + 1
        if "Rebuy ROI" not in headers:
            stats_ws.update_cell(1, rebuy_roi_col, "Rebuy ROI")

        updated_count = 0
        for i, row in enumerate(stats_data, start=2):  # start=2 to skip header
            token = row.get("Token", "").strip().upper()
            if not token:
                continue

            match = next((r for r in log_data if r.get("Token", "").strip().upper() == token), None)
            if not match:
                continue

            rebuy_roi = match.get("Follow-up ROI", "")
            if rebuy_roi and str(rebuy_roi).strip().replace('%', '').replace('.', '', 1).lstrip('-').isdigit():
                stats_ws.update_cell(i, rebuy_roi_col, rebuy_roi)
                print(f"✅ {token} → Rebuy ROI = {rebuy_roi}")
                updated_count += 1

        print(f"✅ Rebuy ROI sync complete: {updated_count} tokens updated.")

    except Exception as e:
        print(f"❌ Error in run_rebuy_roi_tracker: {e}")
