# sentiment_alerts.py

import os
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_prompt, ping_webhook_debug

def run_sentiment_alerts():
    print("🔔 Running High-Hype Sentiment Alerts...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        summary_ws = sheet.worksheet("Sentiment_Summary")
        alerts_ws = sheet.worksheet("Sentiment_Alerts")

        summary_data = summary_ws.get_all_records()
        alerts_data = alerts_ws.get_all_records()
        recent_alerts = {
            (row["Token"], row["Alert Type"]): datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
            for row in alerts_data
        }

        sent_count = 0
        now = datetime.utcnow()

        for row in summary_data[-50:]:  # Only check last 50 entries
            token = row.get("Token", "").strip().upper()
            alert_type = row.get("Alert", "").strip()
            mentions = int(row.get("Total Mentions", 0))
            score = float(row.get("Signal Score", 0))

            if alert_type != "🔥 HIGH HYPE" or mentions < 30:
                continue

            alert_key = (token, alert_type)
            last_alert_time = recent_alerts.get(alert_key)

            if last_alert_time and (now - last_alert_time).total_seconds() < 86400:
                print(f"⏩ Skipping {token}: recently alerted.")
                continue

            msg = (
                f"🔥 *{token}* is trending across YouTube/Twitter!\n"
                f"*Mentions:* {mentions}\n"
                f"*Score:* {score}\n\n"
                f"Would you like to scout this token?"
            )

            send_telegram_prompt(token, msg, buttons=["YES", "NO"], prefix="HYPE")
            alerts_ws.append_row([
                now.strftime("%Y-%m-%d %H:%M:%S"),
                token,
                alert_type,
                mentions,
                score
            ], value_input_option="USER_ENTERED")

            print(f"✅ Alert sent for {token}")
            sent_count += 1

        print(f"📊 Sentiment alert check complete. {sent_count} alerts sent.")

    except Exception as e:
        print(f"❌ Error in run_sentiment_alerts: {e}")
        ping_webhook_debug(f"❌ Sentiment Alert error: {e}")
