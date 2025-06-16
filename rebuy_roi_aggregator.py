# rebuy_roi_aggregator.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from statistics import mean

def run_rebuy_roi_aggregator():
    print("📊 Syncing Rebuy Performance Stats → Rotation_Stats...")

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

        # Ensure required columns exist
        count_col = headers.index("Rebuy Count") + 1 if "Rebuy Count" in headers else len(headers) + 1
        max_col = headers.index("Max Rebuy ROI") + 1 if "Max Rebuy ROI" in headers else len(headers) + 2
        avg_col = headers.index("Avg Rebuy ROI") + 1 if "Avg Rebuy ROI" in headers else len(headers) + 3

        if "Rebuy Count" not in headers:
            stats_ws.update_cell(1, count_col, "Rebuy Count")
        if "Max Rebuy ROI" not in headers:
            stats_ws.update_cell(1, max_col, "Max Rebuy ROI")
        if "Avg Rebuy ROI" not in headers:
            stats_ws.update_cell(1, avg_col, "Avg Rebuy ROI")

        # Build a map of token → list of Rebuy ROI values
        roi_map = {}
        for row in log_data:
            token = row.get("Token", "").strip().upper()
            roi_val = row.get("Rebuy ROI")
            if not token or roi_val in ["", None]:
                continue
            try:
                roi = float(str(roi_val).strip())
                roi_map.setdefault(token, []).append(roi)
            except:
                continue

        # Update stats
        updated = 0
        for i, row in enumerate(stats_data, start=2):
            token = row.get("Token", "").strip().upper()
            if not token or token not in roi_map:
                continue

            roi_list = roi_map[token]
            count = len(roi_list)
            max_roi = max(roi_list)
            avg_roi = round(mean(roi_list), 2)

            stats_ws.update_cell(i, count_col, count)
            stats_ws.update_cell(i, max_col, max_roi)
            stats_ws.update_cell(i, avg_col, avg_roi)
            print(f"🔁 {token}: Count={count}, Max={max_roi}, Avg={avg_roi}")
            updated += 1

        print(f"✅ Rebuy ROI aggregation complete: {updated} tokens updated.")

    except Exception as e:
        print(f"❌ Error in run_rebuy_roi_aggregator: {e}")
