import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def run_rotation_stats_sync():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    log_ws = sheet.worksheet("Rotation_Log")
    stats_ws = sheet.worksheet("Rotation_Stats")

    log_data = log_ws.get_all_records()
    stats_data = stats_ws.get_all_records()
    stats_tokens = [(row["Token"], row["Date"]) for row in stats_data]

    for row in log_data:
        token = row.get("Token", "").strip()
        entry_date = row.get("Timestamp", "").strip()
        followup = row.get("Follow-up ROI", "").strip()

        if not token or not entry_date:
            continue

        if (token, entry_date) in stats_tokens:
            continue

        try:
            initial_roi = float(row.get("Score", ""))
            followup_roi = float(followup) if followup and followup != "N/A" else None
            performance = round(followup_roi / initial_roi, 2) if followup_roi else "N/A"
        except:
            initial_roi = "N/A"
            performance = "N/A"

        days_held = row.get("Days Held", "")

        stats_ws.append_row([
            entry_date, token, "YES", initial_roi, row.get("Sentiment", ""),
            row.get("Status", ""), days_held, followup, performance, "", ""
        ])

        print(f"✅ Synced {token} to Rotation_Stats")
