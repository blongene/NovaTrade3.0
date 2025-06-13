# vault_review_alerts.py

import os
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_prompt, get_gspread_client


def run_vault_review_alerts():
    print("🔔 Running Vault Review Alerts...")

    try:
        # Authenticate and open sheet
        client = get_gspread_client()
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        vault_ws = sheet.worksheet("Token_Vault")
        memory_ws = sheet.worksheet("Vault_Memory")
        vault_data = vault_ws.get_all_records()
        memory_data = memory_ws.get_all_records()

        today = datetime.utcnow()
        sent_count = 0

        for row in vault_data:
            token = row.get("Token", "").strip().upper()
            roi_str = str(row.get("Vault ROI", "")).strip()
            memory_tag = row.get("Memory Tag", "")
            last_reviewed = row.get("Last Reviewed", "")
            vault_tag = row.get("Vault Tag", "")

            if not token or not roi_str or not vault_tag:
                continue

            # Must be Vaulted AND tagged Big Win
            if vault_tag != "✅ Vaulted" or memory_tag != "🟢 Big Win":
                continue

            # Must have ROI >= 200
            try:
                roi = float(roi_str)
            except:
                continue

            if roi < 200:
                continue

            # Must be unreviewed in past 7 days
            days_since_review = 999
            if last_reviewed:
                try:
                    dt = datetime.strptime(last_reviewed, "%Y-%m-%dT%H:%M:%S")
                    days_since_review = (today - dt).days
                except:
                    pass

            if days_since_review < 7:
                continue

            # Check if already logged in Vault_Memory
            if any(m.get("Token", "").strip().upper() == token and m.get("Decision") for m in memory_data):
                continue

            # Send Telegram alert
            message = (
                f"🧠 *{token}* was vaulted and returned +{roi}% over {row.get('Days Held', '?')} days.\n"
                f"Would you vault this token again today?"
            )
            send_telegram_prompt(token, message, buttons=["YES", "NO"], prefix="VAULT REVIEW")
            sent_count += 1

        print(f"✅ Vault Review check complete. {sent_count} prompt(s) sent.")

    except Exception as e:
        print(f"❌ Error in run_vault_review_alerts: {e}")
