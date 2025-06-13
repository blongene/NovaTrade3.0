# vault_alerts_phase15d.py

import os
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_prompt, ping_webhook_debug

@staticmethod
def run_vault_alerts():
    print("🔔 Running Vault Intelligence Alerts...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        stats_ws = sheet.worksheet("Rotation_Stats")
        vault_ws = sheet.worksheet("Token_Vault")

        stats_data = stats_ws.get_all_records()
        vault_data = vault_ws.get_all_records()

        now = datetime.utcnow()
        sent_count = 0

        for stat in stats_data:
            token = stat.get("Token", "").strip().upper()
            tag = stat.get("Vault Tag", "")
            last_seen = stat.get("Last Seen", "") or stat.get("Last Reviewed", "")

            # Alert: Previously Vaulted but has ROI or new mention
            if "Previously" in tag:
                roi = stat.get("Follow-up ROI", "")
                try:
                    if float(roi) >= 5:
                        send_telegram_prompt(
                            token,
                            f"$${token} was previously vaulted, but is showing new signs of life (ROI {roi}%). Would you like to unvault it?",
                            buttons=["YES", "NO"],
                            prefix="UNVAULT"
                        )
                        sent_count += 1
                        continue
                except:
                    pass

            # Alert: Vaulted too long with no update
            if tag == "✅ Vaulted":
                try:
                    dt = datetime.strptime(last_seen, "%Y-%m-%dT%H:%M:%S")
                    if (now - dt).days >= 30:
                        send_telegram_prompt(
                            token,
                            f"$${token} has been in the vault for 30+ days with no update. Still a long-term hold?",
                            buttons=["YES", "NO"],
                            prefix="VAULT CHECK"
                        )
                        sent_count += 1
                except Exception as e:
                    print(f"⚠️ Date parse error for {token}: {e}")
                    continue

        print(f"✅ Vault alert check complete. {sent_count} Telegram(s) sent.")

    except Exception as e:
        print(f"❌ Error in run_vault_alerts: {e}")
        ping_webhook_debug(f"❌ vault_alerts error: {e}")
