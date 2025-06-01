from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def scan_roi_tracking():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "sentiment-log-service.json", scope
        )
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        rotation_log = sheet.worksheet("Rotation_Log")

        rows = rotation_log.get_all_records()
        now = datetime.utcnow()

        for i, row in enumerate(rows):
            entry = row.get("Timestamp", "")
            if not entry:
                continue

            try:
                # Accept both ISO and standard format
                if "T" in entry:
                    ts = datetime.fromisoformat(entry.replace("Z", ""))
                else:
                    ts = datetime.strptime(entry, "%m/%d/%Y %H:%M:%S")

                days = (now - ts).days
                rotation_log.update_cell(i + 2, 9, days)  # "Days Held"
                rotation_log.update_cell(i + 2, 10, f"{days}d since vote")  # "Follow-up ROI"
                print(f"🔁 Updated ROI tracker for row {i + 2} — {days}d since vote")

            except Exception as e:
                print(f"⚠️ Failed to update row {i + 2}: {e}")

    except Exception as e:
        print(f"❌ ROI tracking error: {e}")
