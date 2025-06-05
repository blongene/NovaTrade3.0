import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime

def run_rotation_log_updater():
    print("\U0001f4dd Running rotation_log_updater...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        review_ws = sheet.worksheet("ROI_Review_Log")

        log_data = log_ws.get_all_records()
        review_data = review_ws.get_all_records()
        log_headers = log_ws.row_values(1)

        token_idx = log_headers.index("Token")
        roi_idx = log_headers.index("Follow-up ROI")

        updates = 0

        for i, row in enumerate(log_data):
            token = row.get("Token", "").strip()
            current_val = str(row.get("Follow-up ROI", "")).strip()

            # Skip if already numeric
            try:
                float(current_val)
                continue
            except:
                pass

            # Try to find a matching numeric ROI in review tab
            match = next((r for r in review_data if r["Token"].strip() == token), None)
            if match:
                new_val = str(match.get("ROI", "")).strip()
                try:
                    float(new_val)
                    log_ws.update_cell(i + 2, roi_idx + 1, new_val)
                    updates += 1
                except:
                    continue

        print(f"✅ Follow-up ROI patch complete. {updates} cell(s) updated.")

    except Exception as e:
        print(f"❌ rotation_log_updater failed: {e}")
