import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime
from utils import send_telegram_prompt

def run_rebalance_scanner():
    print("📊 Running Rebalance Scanner...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        target_ws = sheet.worksheet("Portfolio_Targets")
        rows = target_ws.get_all_records()

        for row in rows:
            token = row.get("Token", "").strip()
            status = row.get("Notes", "").strip().lower()

            if status in ["overweight", "undersized"]:
                message = f"⚖️ *Rebalance Candidate Detected: {token}*\n– Status: {status.title()}\n\nWould you like to adjust this holding to meet target allocations?"
        message = f"Token *{token}* has hit rebalance criteria.\n\nScore: {score}\nSentiment: {sentiment}\nMarket Cap: {market_cap}"
        send_telegram_prompt(token, message, buttons=["ROTATE", "HOLD"], prefix="REBALANCE")

        print("✅ Rebalance scanner complete.")

    except Exception as e:
        print(f"❌ rebalance_scanner failed: {e}")
