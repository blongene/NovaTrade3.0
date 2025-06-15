
# memory_weight_sync.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_memory_weight_sync():
    print("🔁 Syncing Memory Weights...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        memory_ws = sheet.worksheet("Rotation_Memory")

        records = memory_ws.get_all_records()
        headers = memory_ws.row_values(1)

        weight_col = headers.index("Memory Weight") + 1

        for i, row in enumerate(records, start=2):  # Start at row 2
            token = row.get("Token", "").strip().upper()
            try:
                wins = int(row.get("Wins", 0))
                losses = int(row.get("Losses", 0))
                win_rate_str = str(row.get("Win Rate", "0%")).replace("%", "").strip()
                win_rate = float(win_rate_str) / 100.0 if win_rate_str else 0.0

                weight = round((wins - losses) * win_rate, 2)
                memory_ws.update_cell(i, weight_col, weight)
                print(f"🧠 {token} → Memory Weight = {weight}")
            except Exception as e:
                print(f"⚠️ Skipped {token}: {e}")

        print("✅ Memory Weight sync complete.")

    except Exception as e:
        print(f"❌ Error in run_memory_weight_sync: {e}")
