import gspread
import os
from datetime import datetime
from utils import send_rotation_alert
from oauth2client.service_account import ServiceAccountCredentials


def run_vault_review_alerts():
    print("📬 Running Vault Review Alerts...")
    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        roi_ws = sheet.worksheet("Vault_ROI_Tracker")
        memory_ws = sheet.worksheet("Vault_Memory")

        roi_rows = roi_ws.get_all_records()
        memory_rows = memory_ws.get_all_records()
        prompted_tokens = {row['Token'] for row in memory_rows if row.get('Decision')}

        recent_rows = [row for row in roi_rows if row.get('Date')]
        if not recent_rows:
            print("⚠️ No recent vault ROI data found.")
            return

        latest_date = max(row['Date'] for row in recent_rows)
        recent = [row for row in recent_rows if row['Date'] == latest_date]

        for row in recent:
            token = row.get("Token", "").strip()
            roi = float(row.get("ROI", 0))

            if not token or roi < 200 or token in prompted_tokens:
                continue

            # Prompt message
            message = (
                f"📦 *{token}* is still vaulted after reaching ROI of {roi:.1f}%!\n\n"
                f"Would you still vote YES to keep it vaulted, or rotate it out?"
            )
            send_rotation_alert(token, message, context="Vault Review")
            memory_ws.append_row([
                datetime.utcnow().isoformat(),
                token,
                "",  # Awaiting decision
                "Prompt Sent"
            ], value_input_option="USER_ENTERED")
            print(f"🔔 Vault review alert sent for {token}")

    except Exception as e:
        print(f"❌ Error in run_vault_review_alerts: {e}")
