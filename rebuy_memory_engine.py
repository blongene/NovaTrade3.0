# === rebuy_memory_engine.py (patched) ===
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import safe_float

def run_rebuy_memory_engine():
    print("🔁 Running Memory-Aware Rebuy Scan...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")
        data = stats_ws.get_all_records()

        valid_tokens = []

        for row in data:
            token = str(row.get("Token", "")).strip().upper()
            if not token:
                continue

            memory_score = safe_float(row.get("Total Memory Score", 0))
            if memory_score > 3:
                valid_tokens.append((token, memory_score))

        print("✅ Rebuy memory scan complete.")
        return valid_tokens

    except Exception as e:
        print(f"❌ Error in run_rebuy_memory_engine: {e}")
