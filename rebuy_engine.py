# rebuy_engine.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import safe_float, send_telegram_prompt

def run_rebuy_engine():
    print("🔁 Running undersized rebuy engine...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            raise ValueError("SHEET_URL not set")

        sheet = client.open_by_url(sheet_url)
        vault_ws = sheet.worksheet("Token_Vault")

        vault_data = vault_ws.get_all_records()
        rebuy_threshold = safe_float(os.getenv("REBUY_USDT_THRESHOLD", 200))

        for i, row in enumerate(vault_data, start=2):
            token = str(row.get("Token", "")).strip().upper()
            allocation = safe_float(row.get("USDT Allocated", "0"))
            active = str(row.get("Active", "")).strip().upper()

            if not token or active != "YES":
                continue

            if allocation < rebuy_threshold:
                send_telegram_prompt(
                    token,
                    f"📉 {token} is undersized with only ${allocation:.2f} allocated. Rebuy more?",
                    buttons=["YES", "NO"],
                    prefix="REBUY"
                )
                print(f"⚠️ Undersized: {token} → ${allocation:.2f}")

        print("✅ Rebuy scan complete.")

    except Exception as e:
        print(f"❌ Rebuy engine error: {e}")
