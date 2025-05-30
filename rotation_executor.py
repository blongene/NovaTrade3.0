import gspread
import os
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.environ.get("SHEET_URL")

# Auth
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_url(SHEET_URL)

planner_ws = sheet.worksheet("Rotation_Planner")
log_ws = sheet.worksheet("Rotation_Log")

def sync_confirmed_to_rotation_log():
    planner = planner_ws.get_all_records()
    log_tokens = [row[1].strip().upper() for row in log_ws.get_all_values()[1:]]  # Token column in Rotation_Log

    for i, row in enumerate(planner):
        token = row.get("Token", "").strip().upper()
        confirmed = row.get("Confirmed", "").strip().upper()

        if not token or confirmed != "YES":
            continue
        if token in log_tokens:
            continue

        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        source = row.get("Source", "Telegram")
        score = row.get("Score", "")
        sentiment = row.get("Sentiment", "")
        market_cap = row.get("Market Cap", "")
        scout_url = row.get("Scout URL", "")
        allocation = "TBD"

        new_row = [timestamp, token, "Active", score, sentiment, market_cap, scout_url, allocation]
        log_ws.append_row(new_row, value_input_option="USER_ENTERED")
        print(f"✅ Synced to Rotation_Log: {token}")
