# roi_threshold_validator.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_roi_threshold_validator():
    print("🔎 Running ROI Threshold Validator...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        memory_ws = sheet.worksheet("Rotation_Memory")
        stats_ws = sheet.worksheet("Rotation_Stats")

        memory_data = memory_ws.get_all_records()
        stats_data = stats_ws.get_all_records()

        headers = memory_ws.row_values(1)
        eligible_col = headers.index("Eligible") + 1 if "Eligible" in headers else len(headers) + 1

        if "Eligible" not in headers:
            memory_ws.update_cell(1, eligible_col, "Eligible")

        # Create lookup dictionary for latest ROI values by token
        roi_lookup = {
            row.get("Token", "").strip().upper(): row.get("Follow-up ROI")
            for row in stats_data
        }

        for i, row in enumerate(memory_data, start=2):
            token = row.get("Token", "").strip().upper()
            roi_val = roi_lookup.get(token)

            try:
                roi = float(str(roi_val).replace("%", "").strip())
                eligible = "Yes" if roi >= 10 else "No"
                memory_ws.update_cell(i, eligible_col, eligible)
                print(f"✅ {token} → ROI {roi}% → Eligible = {eligible}")
            except:
                memory_ws.update_cell(i, eligible_col, "No")
                print(f"⚠️ {token} → Invalid or missing ROI")

        print("✅ ROI Threshold Validator complete.")

    except Exception as e:
        print(f"❌ Error in run_roi_threshold_validator: {e}")
