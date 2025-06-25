# total_memory_score_sync.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def sync_total_memory_score():
    try:
        print("🧠 Calculating Total Memory Score...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet_url = os.getenv("SHEET_URL")
        sheet = client.open_by_url(sheet_url)
        stats_ws = sheet.worksheet("Rotation_Stats")

        data = stats_ws.get_all_records()
        headers = data[0].keys()

        token_col = list(headers).index("Token") + 1
        memory_col = list(headers).index("Memory Score") + 1
        rebuy_col = list(headers).index("Rebuy Weight") + 1

        try:
            total_col = list(headers).index("Total Memory Score") + 1
        except ValueError:
            # Insert new column if it doesn't exist
            stats_ws.insert_cols([["Total Memory Score"]], col=len(headers)+1)
            total_col = len(headers)+1

        for i, row in enumerate(data, start=2):
            token = row.get("Token", "").strip().upper()
            memory = row.get("Memory Score", "")
            rebuy = row.get("Rebuy Weight", "")

            try:
                memory = float(memory)
            except:
                memory = 0.0

            try:
                rebuy = float(rebuy)
            except:
                rebuy = 0.0

            total_score = round(memory + rebuy, 2)
            stats_ws.update_acell(f"{gspread.utils.rowcol_to_a1(total_col, i)}", total_score)
            print(f"✅ {token} → Total Score = {total_score}")

        print("✅ Total Memory Score sync complete.")

    except Exception as e:
        print(f"❌ Error in sync_total_memory_score: {e}")
