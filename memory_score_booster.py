# memory_score_booster.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_memory_boost(token_name):
    try:
        token = token_name.strip().upper()
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")
        rows = stats_ws.get_all_records()

        for row in rows:
            if row.get("Token", "").strip().upper() == token:
                mem_score = row.get("Memory Score", "")
                try:
                    score = int(mem_score)
                    if score >= 3:
                        print(f"🧠 Memory boost for {token}: +2 (score={score})")
                        return 2
                    elif score <= -3:
                        print(f"🧠 Memory penalty for {token}: -2 (score={score})")
                        return -2
                    else:
                        print(f"🧠 No memory adjustment for {token} (score={score})")
                        return 0
                except:
                    print(f"⚠️ Invalid Memory Score for {token}: {mem_score}")
                    return 0

        print(f"🧠 {token} not found in Rotation_Stats — no memory applied")
        return 0

    except Exception as e:
        print(f"❌ Memory boost error for {token_name}: {e}")
        return 0
