# nova_heartbeat.py

import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def log_heartbeat(module="System", message="Heartbeat confirmed"):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "sentiment-log-service.json", scope
        )
        client = gspread.authorize(creds)

        sheet_url = os.environ.get("SHEET_URL")
        if not sheet_url:
            raise ValueError("SHEET_URL environment variable is not set.")

        sheet = client.open_by_url(sheet_url)
        heartbeat_tab = sheet.worksheet("NovaHeartbeat")

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        heartbeat_tab.append_row([now, module, message], value_input_option="USER_ENTERED")

        print(f"✅ NovaHeartbeat log: [{module}] {message}")

    except Exception as e:
        print(f"❌ Failed to write to NovaHeartbeat: {e}")
