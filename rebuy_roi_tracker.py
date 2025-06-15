# rebuy_roi_tracker.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials


def run_rebuy_roi_tracker():
    print("📈 Running Rebuy ROI Tracker...")

    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        log_data = log_ws.get_all_records()
        stats_data = stats_ws.get_all_records()
        stats_headers = stats_ws.row_values(1)

        rebuy_col = stats_headers.index("Rebuy ROI") + 1 if "Rebuy ROI" in stats_headers else len(stats_headers) + 1
        if "Rebuy ROI" not in stats_headers:
            stats_ws.update_cell(1, rebuy_col, "Rebuy ROI")

        for i, row in enumerate(stats_data, start=2):
            token = row.get("Token", "").strip().upper()
            if not token:
                continue

            # Match to Rotation_Log for rebuy ROI value
            log_entry = next(
                (r for r in log_data if r.get("Token", "").strip().upper() == token),
                None
            )
            if not log_entry:
                continue

            rebuy_roi = log_entry.get("Rebuy ROI", "").strip()
            if rebuy_roi and rebuy_roi not in ["N/A", "", None]:
                stats_ws.update_cell(i, rebuy_col, rebuy_roi)
                print(f"🔁 {token} → Rebuy ROI = {rebuy_roi}")

        print("✅ Rebuy ROI sync complete.")

    except Exception as e:
        print(f"❌ Error in run_rebuy_roi_tracker: {e}")
