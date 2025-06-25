# vault_memory_evaluator.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import safe_float

def run_vault_memory_evaluator():
    print("🧠 Evaluating Vault Memory Scores...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        stats_ws = sheet.worksheet("Rotation_Stats")

        headers = stats_ws.row_values(1)
        data = stats_ws.get_all_records()

        # Ensure the column exists
        vault_score_col = headers.index("Memory Vault Score") + 1 if "Memory Vault Score" in headers else len(headers) + 1
        if "Memory Vault Score" not in headers:
            stats_ws.update_cell(1, vault_score_col, "Memory Vault Score")

        for i, row in enumerate(data, start=2):
            token = str(row.get("Token", "")).strip().upper()
            vault_tag = str(row.get("Vault Tag", "")).strip()
            if vault_tag != "✅ Vaulted":
                stats_ws.update_cell(i, vault_score_col, "")
                continue

            mem_score = safe_float(row.get("Memory Score", 0))
            rebuy_roi = safe_float(row.get("Avg Rebuy ROI", 0))
            rebuy_count = safe_float(row.get("Rebuy Count", 0))

            score = round(mem_score + (rebuy_roi / 50.0) + (rebuy_count * 0.2), 2)
            stats_ws.update_cell(i, vault_score_col, score)
            print(f"✅ {token} → Memory Vault Score = {score}")

        print("✅ Vault Memory Evaluation complete.")

    except Exception as e:
        print(f"❌ Error in run_vault_memory_evaluator: {e}")
