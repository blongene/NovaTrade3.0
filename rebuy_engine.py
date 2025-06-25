# rebuy_engine.py (patched)

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_prompt, is_valid_token

def run_rebuy_engine():
    print("🔁 Running undersized rebuy engine...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        ws = sheet.worksheet("Rotation_Stats")
        data = ws.get_all_records()

        for i, row in enumerate(data, start=2):
            token = str(row.get("Token", "")).strip().upper()
            if not is_valid_token(token):
                continue

            weight = row.get("Rebuy Weight", 0)
            if isinstance(weight, str):
                try:
                    weight = float(weight.strip())
                except:
                    weight = 0

            if weight < 0.3:  # rebuy weight threshold
                prompt = f"💸 {token} has a low Rebuy Weight ({weight}). Should we rebuy while it's cheap?"
                send_telegram_prompt(token, prompt, buttons=["YES", "NO"], prefix="REBUY")

        print("✅ Rebuy engine scan complete.")

    except Exception as e:
        print(f"❌ Rebuy engine error: {e}")
