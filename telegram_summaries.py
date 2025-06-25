# telegram_summaries.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message

def run_telegram_summary():
    print("📢 Running Telegram Summary Layer...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        stats_ws = sheet.worksheet("Rotation_Stats")
        stats_data = stats_ws.get_all_records()

        summary_lines = []

        def safe_str(val):
            return str(val).strip() if val is not None else ""

        for row in stats_data:
            token = safe_str(row.get("Token", "")).upper()
            performance = safe_str(row.get("Performance", ""))
            roi = safe_str(row.get("Follow-up ROI", ""))
            tag = safe_str(row.get("Vault Tag", ""))
            score = safe_str(row.get("Total Memory Score", ""))

            if not token:
                continue

            line = f"🪙 {token} | ROI: {roi}% | Score: {score} | Status: {tag or '—'}"
            summary_lines.append(line)

        if not summary_lines:
            print("⚠️ No valid tokens found for summary.")
            return

        message = "*Nova Rotation Summary:*\n\n" + "\n".join(summary_lines[:20])  # Limit to top 20
        send_telegram_message(message)

        print("✅ Telegram Summary sent.")

    except Exception as e:
        print(f"❌ Telegram summary error: {e}")
