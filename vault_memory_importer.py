# vault_memory_importer.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from vault_memory_evaluator import run_vault_memory_evaluator

def run_vault_memory_importer():
    print("📥 Importing Vault Memory Scores...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        stats_ws = sheet.worksheet("Rotation_Stats")
        rows = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        token_col = headers.index("Token") + 1
        score_col = headers.index("Memory Vault Score") + 1 if "Memory Vault Score" in headers else len(headers) + 1

        # If column doesn't exist, add it
        if "Memory Vault Score" not in headers:
            stats_ws.update_cell(1, score_col, "Memory Vault Score")

        for i, row in enumerate(rows, start=2):
            token = row.get("Token", "").strip()
            if not token:
                continue

            score = evaluate_vault_memory(token)["memory_score"]
            stats_ws.update_cell(i, score_col, score)
            print(f"✅ {token} → Memory Vault Score = {score}")

        print("✅ Vault Memory Evaluation complete.")

    except Exception as e:
        print(f"❌ Error in run_vault_memory_importer: {e}")
