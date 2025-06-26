# memory_score_sync.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_memory_score_sync():
    print("🧠 Calculating Total Memory Score...")

    try:
        # Google Sheets auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        stats_ws = sheet.worksheet("Rotation_Stats")
        rows = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        memory_col = headers.index("Memory Score") + 1
        rebuy_weight_col = headers.index("Rebuy Weight") + 1
        vault_score_col = headers.index("Memory Vault Score") + 1 if "Memory Vault Score" in headers else None
        total_col = headers.index("Total Memory Score") + 1 if "Total Memory Score" in headers else len(headers) + 1

        if "Total Memory Score" not in headers:
            stats_ws.update_cell(1, total_col, "Total Memory Score")

        for i, row in enumerate(rows, start=2):
            try:
                m_score = float(row.get("Memory Score", 0))
                r_weight = float(row.get("Rebuy Weight", 0))
                v_score = float(row.get("Memory Vault Score", 0)) if vault_score_col else 0

                total = round(m_score + r_weight + v_score, 2)
                stats_ws.update_cell(i, total_col, total)
                print(f"✅ {row.get('Token', '')} → Total Score = {total}")

            except Exception as e:
                print(f"⚠️ Row {i} skipped: {e}")

        print("✅ Total Memory Score sync complete.")

    except Exception as e:
        print(f"❌ Error in run_memory_score_sync: {e}")
