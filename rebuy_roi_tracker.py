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

        stats_ws = sheet.worksheet("Rotation_Stats")
        stats_data = stats_ws.get_all_records()
        token_index = {str(row.get("Token", "")).strip().upper(): i+2 for i, row in enumerate(stats_data)}

        memory_ws = sheet.worksheet("Rotation_Memory")
        memory_data = memory_ws.get_all_records()

        for row in memory_data:
            token = str(row.get("Token", "")).strip().upper()
            roi = row.get("Rebuy ROI", "")
            count = row.get("Rebuy Count", "")
            max_roi = row.get("Max Rebuy ROI", "")
            avg_roi = row.get("Avg Rebuy ROI", "")

            if token not in token_index:
                continue

            row_num = token_index[token]
            stats_ws.update_acell(f"N{row_num}", roi)
            stats_ws.update_acell(f"O{row_num}", count)
            stats_ws.update_acell(f"P{row_num}", max_roi)
            stats_ws.update_acell(f"Q{row_num}", avg_roi)

            print(f"✅ {token} → Rebuy ROI = {roi}, Count = {count}, Max = {max_roi}, Avg = {avg_roi}")

        print(f"✅ Rebuy ROI sync complete: {len(token_index)} tokens updated.")

    except Exception as e:
        print(f"❌ Error in run_rebuy_roi_tracker: {e}")
