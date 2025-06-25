# vault_confidence_score.py

import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from utils import safe_float

def calculate_confidence(token):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")
        records = stats_ws.get_all_records()

        for row in records:
            row_token = str(row.get("Token", "")).strip().upper()
            if row_token != token.upper():
                continue

            score = safe_float(row.get("Memory Vault Score", 0))
            # Map score to confidence %
            if score >= 5:
                return 90
            elif score >= 3:
                return 70
            elif score >= 1:
                return 50
            else:
                return 20

        return 0  # fallback if token not found

    except Exception as e:
        print(f"❌ Confidence Score error for {token}: {e}")
        return 0
