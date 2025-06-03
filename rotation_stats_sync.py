import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")

def run_rotation_stats_sync():
    try:
        # Setup auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(SHEET_URL)

        log_ws = sheet.worksheet("Rotation_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        log_data = log_ws.get_all_records()
        stats_data = stats_ws.get_all_records()
        existing_tokens = [row.get("Token", "").strip().upper() for row in stats_data]

        for row in log_data:
            token = row.get("Token", "").strip().upper()
            if token in existing_tokens:
                continue

            date = row.get("Timestamp", "")
            decision = "YES"
            sentiment = row.get("Sentiment", "")
            status = row.get("Status", "Active")
            days_held = row.get("Days Held", 0)
            follow_up_roi = row.get("Follow-up ROI", "")
            initial_roi = ""  # Optional: fetch from scout or ROI tab if needed

            # Compute basic performance tag
            perf = ""
            try:
                roi_val = float(follow_up_roi.strip('%'))
                if roi_val >= 50:
                    perf = "🏆 Win"
                elif roi_val <= -25:
                    perf = "❌ Loss"
                else:
                    perf = "🟡 Breakeven"
            except:
                perf = "—"

            stats_ws.append_row([
                date, token, decision, initial_roi, sentiment, status, days_held, follow_up_roi, perf
            ], value_input_option="USER_ENTERED")

            print(f"📊 Logged to Rotation_Stats: {token}")

    except Exception as e:
        print(f"❌ rotation_stats_sync failed: {e}")

