# scout_to_planner_sync.py

import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials

def sync_rotation_planner():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            raise ValueError("SHEET_URL not set.")

        sheet = client.open_by_url(sheet_url)
        scout_ws = sheet.worksheet("Scout Decisions")
        planner_ws = sheet.worksheet("Rotation_Planner")

        scout = scout_ws.get_all_records()
        planner_tokens = {r.get("Token", "").strip().upper() for r in planner_ws.get_all_records() if r.get("Token")}

        for row in scout:
            token = row.get("Token", "").strip().upper()
            decision = row.get("Decision", "").strip().upper()

            if not token or decision != "YES" or token in planner_tokens:
                continue

            timestamp = row.get("Timestamp", "")
            source = row.get("Source", "")
            score = row.get("Score", "")
            sentiment = row.get("Sentiment", "")
            market_cap = row.get("Market Cap", "")
            scout_url = row.get("Scout URL", "")

            planner_ws.append_row([
                token,
                timestamp,
                decision,
                source,
                score,
                sentiment,
                market_cap,
                scout_url,
                "NO"  # Confirmed
            ], value_input_option="USER_ENTERED")

            print(f"✅ Synced to Rotation_Planner: {token}")

    except Exception as e:
        print(f"❌ sync_rotation_planner error: {e}")
