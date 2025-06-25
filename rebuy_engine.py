# rebuy_engine.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_prompt, ping_webhook_debug
from datetime import datetime

def run_rebuy_engine():
    print("🔁 Running undersized rebuy engine...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        vault_ws = sheet.worksheet("Token_Vault")
        data = vault_ws.get_all_records()

        def safe_str(val):
            return str(val).strip() if val is not None else ""

        rebuy_count = 0
        now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

        for i, row in enumerate(data, start=2):
            token = safe_str(row.get("Token", "")).upper()
            weight = safe_str(row.get("Target %", ""))
            rebuy = safe_str(row.get("Last Rebuy", ""))
            confirm = safe_str(row.get("Auto Rebuy", "")).upper()

            try:
                weight_val = float(weight)
            except:
                weight_val = 0.0

            if not token or weight_val >= 5 or confirm == "NO":
                continue

            send_telegram_prompt(
                token,
                f"$$ {token} target weight is only {weight_val}%. Undersized holding. Want to rebuy?",
                buttons=["YES", "NO"],
                prefix="REBUY"
            )

            vault_ws.update_acell(f"D{i}", now_str)  # Last Rebuy
            rebuy_count += 1

        print(f"✅ Rebuy engine complete. {rebuy_count} prompts sent.")

    except Exception as e:
        print(f"❌ Rebuy engine error: {e}")
        ping_webhook_debug(f"❌ rebuy_engine error: {e}")
