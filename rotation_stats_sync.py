import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime
from send_telegram import send_rotation_alert


def clean_roi_value(raw_roi):
    if isinstance(raw_roi, str):
        return float(raw_roi.replace('%', '').strip())
    return float(raw_roi)

def run_rotation_stats_sync():
    print("📋 Syncing Rotation_Stats tab...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        review_ws = sheet.worksheet("ROI_Review_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        log_data = log_ws.get_all_records()
        review_data = review_ws.get_all_records()

        stats = []
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for row in log_data:
            token = str(row.get("Token", "")).strip()
            initial_roi = str(row.get("Score", "")).strip()
            status = str(row.get("Status", "")).strip()
            days_held = str(row.get("Days Held", "")).strip()
            sentiment = str(row.get("Sentiment", "")).strip()

            # Lookup follow-up ROI from review tab
            follow_up = next((r["ROI"] for r in review_data if r["Token"].strip() == token and r.get("ROI")), None)

            # Validate that both initial and follow-up ROI are numeric
            try:
                initial = clean_roi_value(initial_roi)
                follow = clean_roi_value(follow_up)
            except:
                print(f"⚠️ Skipping {token} — non-numeric ROI or missing value (Initial: {initial_roi}, Follow-up: {follow_up})")
                continue

            # Compute performance
            try:
                performance = round((follow - initial) / initial * 100, 2)
            except ZeroDivisionError:
                performance = 0.0

            stats.append([
                now,
                token,
                "YES",
                initial,
                sentiment,
                status,
                days_held,
                follow,
                performance
            ])

        # Write new Rotation_Stats sheet
        headers = [
            "Date", "Token", "Decision", "Initial ROI", "Sentiment", "Status",
            "Days Held", "Follow-up ROI", "Performance"
        ]
        stats_ws.clear()
        stats_ws.append_row(headers)
        if stats:
            stats_ws.append_rows(stats, value_input_option="USER_ENTERED")
        print(f"✅ Rotation_Stats updated: {len(stats)} rows")

    except Exception as e:
        print(f"❌ rotation_stats_sync failed: {e}")
