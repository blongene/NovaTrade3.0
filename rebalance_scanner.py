# rebalance_scanner.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from telegram import Bot

def run_rebalance_scanner():
    print("🔁 Running Rebalance Scanner...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Portfolio_Targets")

        data = ws.get_all_records()
        drift_alerts = []

        for i, row in enumerate(data, start=2):
            try:
                token = row.get("Token", "").strip()
                target = float(row.get("Target %", 0))
                min_pct = float(row.get("Min %", 0))
                max_pct = float(row.get("Max %", 100))
                current = float(row.get("Current %", 0))

                if current < min_pct:
                    drift_status = "Undersized"
                    drift_alerts.append(f"🔽 {token}: {current}% (Target: {target}%)")
                elif current > max_pct:
                    drift_status = "Overweight"
                    drift_alerts.append(f"🔼 {token}: {current}% (Target: {target}%)")
                else:
                    drift_status = "On target"

                ws.update_acell(f"H{i}", drift_status)
            except Exception as row_err:
                print(f"⚠️ Row {i} error: {row_err}")

        if drift_alerts:
            token_list = "\n".join(drift_alerts)
            message = f"📊 *Portfolio Drift Detected!*\n\n{token_list}\n\nReply YES to rebalance or SKIP to ignore."
            send_telegram_ping(message)

        print("✅ Rebalance check complete.")

    except Exception as e:
        print(f"❌ Rebalance Scanner error: {e}")

def send_telegram_ping(message):
    try:
        bot_token = os.getenv("BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        bot = Bot(token=bot_token)
        bot.send_message(chat_id=chat_id, text=message, parse_mode="Markdown")
    except Exception as e:
        print(f"❌ Telegram error: {e}")
