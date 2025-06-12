# rotation_stats_sync.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import re

def run_rotation_stats_sync():
    print("📊 Syncing Rotation_Stats...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        log_data = log_ws.get_all_records()
        stats_data = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        # Determine column positions
        followup_col = headers.index("Follow-up ROI") + 1
        rebuy_col = headers.index("Rebuy ROI") + 1 if "Rebuy ROI" in headers else None
        memory_col = headers.index("Memory Tag") + 1 if "Memory Tag" in headers else len(headers) + 1

        if "Memory Tag" not in headers:
            stats_ws.update_cell(1, memory_col, "Memory Tag")

        for i, row in enumerate(stats_data, start=2):
            token = row.get("Token", "").strip().upper()
            roi_source = "Rotation_Log"

            # Try getting ROI from Rotation_Log
            match = next((r for r in log_data if r.get("Token", "").strip().upper() == token), None)
            roi_val = ""
            if match:
                roi_val = str(match.get("Follow-up ROI", "")).strip()

            # If not valid, fallback to ROI from Rotation_Stats
            if not roi_val or not re.match(r"^-?\d+(\.\d+)?$", roi_val):
                roi_val = str(row.get("Follow-up ROI", "")).strip()
                roi_source = "Rotation_Stats"

            if not roi_val or not re.match(r"^-?\d+(\.\d+)?$", roi_val):
                continue

            roi = float(roi_val)

            # Determine memory tag
            if roi >= 200:
                tag = "🟢 Big Win"
            elif 25 <= roi < 200:
                tag = "✅ Small Win"
            elif -24 <= roi <= 24:
                tag = "⚪ Break-Even"
            elif -70 <= roi < -25:
                tag = "🔻 Loss"
            elif roi <= -71:
                tag = "🔴 Big Loss"
            else:
                tag = ""

            stats_ws.update_cell(i, memory_col, tag)
            print(f"🧠 {token} tagged as {tag} based on ROI = {roi} from {roi_source}")

    except Exception as e:
        print(f"❌ Error in run_rotation_stats_sync: {e}")
