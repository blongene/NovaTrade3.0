# === rebuy_roi_tracker.py (patched with count, max, avg) ===

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

        # Ensure columns exist
        def ensure_col(col_name):
            if col_name not in headers:
                stats_ws.update_cell(1, len(headers) + 1, col_name)
                headers.append(col_name)
            return headers.index(col_name) + 1

        rebuy_roi_col = ensure_col("Rebuy ROI")
        rebuy_count_col = ensure_col("Rebuy Count")
        max_roi_col = ensure_col("Max Rebuy ROI")
        avg_roi_col = ensure_col("Avg Rebuy ROI")

        updated_count = 0
        for i, row in enumerate(stats_data, start=2):
            token = row.get("Token", "").strip().upper()
            if not token:
                continue

            # Gather all matching rows in Rotation_Log
            matching = [r for r in log_data if r.get("Token", "").strip().upper() == token]
            rebuy_values = []
            for m in matching:
                roi_val = m.get("Rebuy ROI", "")
                try:
                    roi = float(str(roi_val).replace("%", "").strip())
                    rebuy_values.append(roi)
                except:
                    continue

            if not rebuy_values:
                continue

            rebuy_count = len(rebuy_values)
            max_roi = max(rebuy_values)
            avg_roi = round(sum(rebuy_values) / rebuy_count, 2)

            # Update all columns
            stats_ws.update_cell(i, rebuy_roi_col, rebuy_values[-1])  # last rebuy ROI
            stats_ws.update_cell(i, rebuy_count_col, rebuy_count)
            stats_ws.update_cell(i, max_roi_col, max_roi)
            stats_ws.update_cell(i, avg_roi_col, avg_roi)
            print(f"✅ {token} → Rebuy ROI = {rebuy_values[-1]}, Count = {rebuy_count}, Max = {max_roi}, Avg = {avg_roi}")
            updated_count += 1

        print(f"✅ Rebuy ROI sync complete: {updated_count} tokens updated.")

    except Exception as e:
        print(f"❌ Error in run_rebuy_roi_tracker: {e}")

