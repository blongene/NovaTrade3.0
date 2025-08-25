# rotation_stats_sync.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import re
from utils import with_sheet_backoff, get_records_cached, get_values_cached

@with_sheet_backoff
def _read_rotation_stats(ws):
    return ws.get_values_cached()

@with_sheet_backoff
def _read_planner(ws):
    return ws.get_records_cached()

def run_rotation_stats_sync():
    print("📊 Syncing Rotation_Stats...")

    try:
        # === Setup ===
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        log_data = log_ws.get_records_cached("Rotation_Log", ttl_s=180)  # 3‑minute cache
        stats_data = stats_ws.get_records_cached("Rotation_Stats", ttl_s=180)  # 3‑minute cache
        headers = stats_ws.row_values(1)

        # === Column Positions ===
        followup_col = headers.index("Follow-up ROI") + 1
        memory_col = headers.index("Memory Tag") + 1 if "Memory Tag" in headers else len(headers) + 1
        perf_col = headers.index("Performance") + 1 if "Performance" in headers else len(headers) + 2

        # Add missing headers if not found
        if "Memory Tag" not in headers:
            stats_ws.update_cell(1, memory_col, "Memory Tag")
        if "Performance" not in headers:
            stats_ws.update_cell(1, perf_col, "Performance")

        for i, row in enumerate(stats_data, start=2):
            token = str(row.get("Token", "")).strip().upper()

            # ✅ SKIP if no valid token
            if not token or token in ["", "N/A", "-", "NONE"] or not token.isalpha():
                continue

            roi_source = "Rotation_Log"

            # === ROI Sync: Prefer Rotation_Log, fallback to Rotation_Stats
            match = next((r for r in log_data if r.get("Token", "").strip().upper() == token), None)
            roi_val = str(match.get("Follow-up ROI", "")).strip() if match else ""
            if not roi_val or not re.match(r"^-?\d+(\.\d+)?$", roi_val):
                roi_val = str(row.get("Follow-up ROI", "")).strip()
                roi_source = "Rotation_Stats"

            if not roi_val or not re.match(r"^-?\d+(\.\d+)?$", roi_val):
                continue  # skip invalid entries

            roi = float(roi_val)

            # === MEMORY TAG
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

            # === PERFORMANCE CALCULATION ===
            try:
                initial_roi = str(row.get("Initial ROI", "")).replace("%", "").strip()
                followup_roi = roi_val  # use the synced value

                if re.match(r"^-?\d+(\.\d+)?$", initial_roi) and re.match(r"^-?\d+(\.\d+)?$", followup_roi):
                    initial = float(initial_roi)
                    followup = float(followup_roi)

                    if initial != 0:
                        perf = round(followup / initial, 2)
                        stats_ws.update_cell(i, perf_col, perf)
                        print(f"📈 {token} performance = {perf}")
            except Exception as e:
                print(f"⚠️ Could not calculate performance for {token}: {e}")

    except Exception as e:
        print(f"❌ Error in run_rotation_stats_sync: {e}")
