import os
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message, get_gspread_client

def run_unlock_horizon_alerts():
    print("\n🔔 Running Unlock Horizon Alerts...")

    try:
        sheet_url = os.getenv("SHEET_URL")
        client = get_gspread_client()
        sheet = client.open_by_url(sheet_url)
        ws = sheet.worksheet("Claim_Tracker")

        data = ws.get_all_records()

        today = datetime.utcnow().date()
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        updated_rows = 0
        alert_count = 0

        for i, row in enumerate(data, start=2):  # Start at row 2 (after header)
            token = row.get("Token", "").strip().upper()
            unlock_date_str = row.get("Unlock Date", "").strip()
            claimed = row.get("Claimed?", "").strip().lower()
            last_alerted = row.get("Last Alerted", "").strip()

            if not token or not unlock_date_str or claimed == "claimed":
                continue

            try:
                unlock_date = datetime.strptime(unlock_date_str, "%Y-%m-%d").date()
            except:
                print(f"⚠️ Invalid date for token {token}: {unlock_date_str}")
                continue

            days_since = (today - unlock_date).days
            ws.update_acell(f"J{i}", days_since)  # Days Since Unlock
            updated_rows += 1

            # Alert if it's past unlock date, not claimed, and hasn't been alerted in last 24h
            if days_since >= 0:
                should_alert = False

                if not last_alerted:
                    should_alert = True
                else:
                    try:
                        last_alerted_dt = datetime.strptime(last_alerted, "%Y-%m-%d %H:%M:%S")
                        hours_since_alert = (datetime.utcnow() - last_alerted_dt).total_seconds() / 3600
                        if hours_since_alert >= 24:
                            should_alert = True
                    except:
                        should_alert = True  # Fallback: alert if date format is invalid

                if should_alert:
                    msg = (
                        f"🗓 *Upcoming Unlock Alert:*\n\n"
                        f"Token: *{token}*\n"
                        f"Expected Unlock Date: {unlock_date.strftime('%Y-%m-%d')}\n"
                        f"Days Since Unlock: {days_since}\n\n"
                        f"Consider checking claim status and updating the sheet if resolved."
                    )
                    send_telegram_message(msg)
                    ws.update_acell(f"K{i}", now_str)  # Last Alerted
                    alert_count += 1

        print(f"✅ Unlock horizon check complete. {updated_rows} rows updated, {alert_count} alerts sent.")

    except Exception as e:
        print(f"❌ Error in run_unlock_horizon_alerts: {e}")
