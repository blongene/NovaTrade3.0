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

        roi_ws = sheet.worksheet("ROI_Review_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        roi_data = roi_ws.get_all_records()
        stats_data = stats_ws.get_all_records()

        updated = 0

        for stat_row in stats_data:
            token = str(stat_row.get("Token", "")).strip().upper()
            rebuy_entries = [r for r in roi_data if str(r.get("Token", "")).strip().upper() == token and str(r.get("Type", "")).strip().upper() == "REBUY"]

            if not rebuy_entries:
                continue

            rois = []
            for entry in rebuy_entries:
                try:
                    roi = float(entry.get("ROI", 0))
                    rois.append(roi)
                except:
                    continue

            if not rois:
                continue

            avg_roi = round(sum(rois) / len(rois), 2)
            max_roi = max(rois)
            count = len(rois)

            row_index = stats_data.index(stat_row) + 2  # Adjust for header row

            stats_ws.update_acell(f"N{row_index}", max_roi)
            stats_ws.update_acell(f"O{row_index}", count)
            stats_ws.update_acell(f"P{row_index}", max_roi)
            stats_ws.update_acell(f"Q{row_index}", avg_roi)

            print(f"✅ {token} → Rebuy ROI = {avg_roi}, Count = {count}, Max = {max_roi}")
            updated += 1

        print(f"✅ Rebuy ROI sync complete: {updated} tokens updated.")

    except Exception as e:
        print(f"❌ Error in run_rebuy_roi_tracker: {e}")
