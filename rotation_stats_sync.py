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

        for i, row in enumerate(stats_data, start=2):  # Row index in stats_ws
            token = row.get("Token", "").strip().upper()
            status = row.get("Status", "").strip().lower()
            decision = row.get("Decision", "").strip().upper()
            followup_cell = stats_ws.find("Follow-up ROI").col
            rebuy_cell = stats_ws.find("Rebuy ROI").col

            # Match against Rotation_Log
            match = next((r for r in log_data if r.get("Token", "").strip().upper() == token), None)
            if not match:
                continue

            roi_val = str(match.get("Follow-up ROI", "")).strip()

            if not roi_val or not re.match(r"^-?\d+(\.\d+)?$", roi_val):
                continue  # Skip blank or invalid ROI

            roi = float(roi_val)

            # If it's a Rebuy entry → log to Rebuy ROI column
            if decision == "REBUY":
                stats_ws.update_cell(i, rebuy_cell, roi)
                print(f"🔁 Logged Rebuy ROI for {token}: {roi}%")
            else:
                stats_ws.update_cell(i, followup_cell, roi)
                print(f"📈 Updated Follow-up ROI for {token}: {roi}%")

    except Exception as e:
        print(f"❌ Error in run_rotation_stats_sync: {e}")
