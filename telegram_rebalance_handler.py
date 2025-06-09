# telegram_rebalance_handler.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from utils import ping_webhook_debug

def handle_rebalance_vote(token, action):
    """
    Called when a user responds to a rebalance alert via Telegram.
    action = "YES" → proceed with rebalancing logic
    action = "NO"  → log rejection and exit
    """
    try:
        print(f"📩 Telegram Rebalance Vote Received: {token} → {action}")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        planner_ws = sheet.worksheet("Rotation_Planner")
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Check if token already exists
        rows = planner_ws.get_all_records()
        match = [r for r in rows if r.get("Token", "").strip().upper() == token.upper()]
        if match:
            row_index = rows.index(match[0]) + 2  # +2 for header + 1-indexed
            planner_ws.update_acell(f"C{row_index}", action.upper())  # Set User Response
            planner_ws.update_acell(f"B{row_index}", now)             # Update Suggestion Date
        else:
            # Append new row if not present
            planner_ws.append_row([
                token,                  # Token
                now,                    # Suggestion Date
                action.upper(),         # User Response
                "Rebalance Alert",      # Source
                "", "", "", "",         # Score, Sentiment, Market Cap, Scout URL
                "NO"                    # Confirmed = NO
            ], value_input_option="USER_ENTERED")

        print(f"✅ Rebalance vote logged: {token} → {action.upper()}")

    except Exception as e:
        print(f"❌ Failed to handle rebalance vote for {token}: {e}")
        ping_webhook_debug(f"❌ Rebalance vote handler error: {e}")
