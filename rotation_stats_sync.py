import os
import gspread
import re
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
        initial_roi = row.get("Score", "").strip()

        if not token or not entry_date:
            continue

        if (token, entry_date) in stats_tokens:
            continue

        # Validate numeric ROI values
        def is_numeric(val):
            return re.match(r"^-?\d+(\.\d+)?$", str(val))

        followup_roi = float(followup) if is_numeric(followup) else None
        init_roi = float(initial_roi) if is_numeric(initial_roi) else None

        if followup_roi is not None and init_roi:
            performance = round(followup_roi / init_roi, 2)
        else:
            performance = "N/A"

        stats_ws.append_row([
            entry_date, token, "YES", init_roi if init_roi else "N/A", row.get("Sentiment", ""),
            row.get("Status", ""), row.get("Days Held", ""), followup if followup_roi else "N/A", performance, "", ""
        ])

        print(f"✅ Synced {token} to Rotation_Stats")
