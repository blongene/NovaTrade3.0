# rotation_executor.py

import gspread
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

def sync_confirmed_to_rotation_log():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            raise ValueError("SHEET_URL not set.")

        sheet = client.open_by_url(sheet_url)
        planner_ws = sheet.worksheet("Rotation_Planner")
        log_ws = sheet.worksheet("Rotation_Log")

        planner = planner_ws.get_all_records()
        log_tokens = {row[1].strip().upper() for row in log_ws.get_all_values()[1:] if row[1]}

        for row in planner:
            token = row.get("Token", "").strip().upper()
            confirmed = row.get("Confirmed", "").strip().upper()

            if not token or confirmed != "YES" or token in log_tokens:
                continue

            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            source = row.get("Source", "Telegram")
            score = row.get("Score", "")
            sentiment = row.get("Sentiment", "")
            market_cap = row.get("Market Cap", "")
            scout_url = row.get("Scout URL", "")
            allocation = "TBD"

            log_ws.append_row([
                timestamp, token, "Active", score, sentiment,
                market_cap, scout_url, allocation
            ], value_input_option="USER_ENTERED")

            print(f"✅ Synced to Rotation_Log: {token}")

    except Exception as e:
        print(f"❌ sync_confirmed_to_rotation_log error: {e}")
