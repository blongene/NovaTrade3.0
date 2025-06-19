# memory_score_sync.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_memory_score_sync():
    print("🧠 Calculating Total Memory Score...")

    try:
        # Auth to Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Rotation_Stats")

        data = ws.get_all_records()
        headers = ws.row_values(1)

        # Ensure column exists
        total_score_col = headers.index("Total Memory Score") + 1 if "Total Memory Score" in headers else len(headers) + 1
        if "Total Memory Score" not in headers:
            ws.update_cell(1, total_score_col, "Total Memory Score")

        updated = 0
        for i, row in enumerate(data, start=2):
            try:
                memory_score = float(str(row.get("Memory Score", 0)).strip())
            except:
                memory_score = 0

            try:
                rebuy_weight = float(str(row.get("Rebuy Weight", 0)).strip())
            except:
                rebuy_weight = 0

            total = round(memory_score + rebuy_weight, 2)
            ws.update_cell(i, total_score_col, total)
            updated += 1
            print(f"✅ {row.get('Token', '')} → Total Score = {total}")

        print(f"✅ Total Memory Score sync complete. {updated} rows updated.")

    except Exception as e:
        print(f"❌ Error in run_memory_score_sync: {e}")
