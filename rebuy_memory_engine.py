# rebuy_memory_engine.py

import os
import re
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message, ping_webhook_debug

def run_memory_rebuy_scan():
    try:
        print("🔁 Running Memory-Aware Rebuy Scan...")

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        log_ws = sheet.worksheet("Rotation_Log")
        radar_ws = sheet.worksheet("Sentiment_Radar")
        vault_ws = sheet.worksheet("Portfolio_Targets")

        log_data = log_ws.get_all_records()
        radar_data = radar_ws.get_all_records()
        vault_data = {r["Symbol"].strip().upper(): float(r.get("Current %", 0)) for r in vault_ws.get_all_records() if r.get("Symbol")}

        for i, row in enumerate(log_data, start=2):  # Row offset for headers
            token = row.get("Token", "").strip().upper()
            status = row.get("Status", "").strip().lower()
            roi = str(row.get("Follow-up ROI", "")).strip()
            prompted = str(row.get("Rebuy Prompted", "")).strip().lower()
            if status != "rotated" or prompted == "yes":
                continue

            # Clean ROI
            roi_val = 0
            if re.match(r"^-?\d+(\.\d+)?$", roi):
                roi_val = float(roi)

            if roi_val < 10:
                continue  # Only consider tokens that had decent performance

            sentiment_hits = 0
            for radar in radar_data:
                radar_token = radar.get("Token", "").strip().upper()
                mentions = int(radar.get("Mentions", 0))
                if radar_token == token and mentions >= 30:
                    sentiment_hits = mentions
                    break

            if sentiment_hits == 0:
                continue  # No sentiment spike

            alloc = vault_data.get(token, 0)
            if alloc >= 2:
                continue  # Not undersized enough to rebuy

            msg = f"🔁 *Rebuy Signal Detected*\n"
            msg += f"Token: ${token}\n"
            msg += f"Past ROI: {roi_val}%\n"
            msg += f"Sentiment Mentions: {sentiment_hits} (🔥)\n"
            msg += f"Current Allocation: {alloc:.2f}%\n"
            msg += f"\nWould you like to re-enter this position?"

            send_telegram_message(msg)

            # Mark as prompted
            log_ws.update_cell(i, log_ws.find("Rebuy Prompted").col, "Yes")

        print("✅ Rebuy memory scan complete.")

    except Exception as e:
        print(f"❌ Error in rebuy memory scan: {e}")
        ping_webhook_debug(f"❌ Error in rebuy memory scan: {e}")
