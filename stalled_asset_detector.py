# stalled_asset_detector.py
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from nova_heartbeat import log_heartbeat
from nova_trigger import trigger_nova_ping  # ✅ Will set NovaTrigger!A1 = SYNC NEEDED

def run_stalled_asset_detector():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "sentiment-log-service.json", scope
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.environ["SHEET_URL"])
        rotation_log = sheet.worksheet("Rotation_Log")
        headers = rotation_log.row_values(1)

        token_col = headers.index("Token") + 1
        days_held_col = headers.index("Days Held") + 1
        roi_col = headers.index("Follow-up ROI") + 1
        status_col = len(headers) + 1  # Next blank column = Stalled Status

        data = rotation_log.get_all_values()[1:]  # skip header
        flagged_tokens = []

        for i, row in enumerate(data, start=2):  # start=2 to account for header row
            try:
                token = row[token_col - 1]
                days_held_str = row[days_held_col - 1].strip()
                roi_str = row[roi_col - 1].strip()

                days_held = int(days_held_str) if days_held_str.isdigit() else 0
                stagnant_roi = "0d" in roi_str or "1d" in roi_str

                if days_held >= 14 and stagnant_roi:
                    status = "⚠️ At Risk: 14d + flat ROI"
                    rotation_log.update_cell(i, status_col, status)
                    flagged_tokens.append(token)

                else:
                    rotation_log.update_cell(i, status_col, "✅ Healthy")

            except Exception as e:
                print(f"⚠️ Skipped row {i} due to error: {e}")
                continue

        # Heartbeat log
        if flagged_tokens:
            log_heartbeat("Stalled Asset Detector", f"Flagged: {', '.join(flagged_tokens)}")
            trigger_nova_ping("SYNC NEEDED")
        else:
            log_heartbeat("Stalled Asset Detector", "All tokens healthy")

        print("✅ Stalled Asset check complete.")

    except Exception as e:
        print(f"❌ Stalled detector error: {e}")
