# vault_memory_importer.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from vault_memory_evaluator import evaluate_vault_memory
from vault_confidence_score import calculate_confidence

def run_vault_memory_importer():
    print("📥 Importing Vault Memory Scores...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        vault_ws = sheet.worksheet("Token_Vault")
        memory_ws = sheet.worksheet("Vault_Memory")

        vault_tokens = {row["Token"].strip().upper() for row in vault_ws.get_all_records() if row.get("Token")}
        existing_tokens = {row["Token"].strip().upper() for row in memory_ws.get_all_records() if row.get("Token")}

        new_rows = []
        for token in sorted(vault_tokens - existing_tokens):
            memory = evaluate_vault_memory(token)
            confidence = calculate_confidence(token)

            new_rows.append([
                datetime.utcnow().isoformat(),
                token,
                confidence,
                memory["avg_roi"],
                memory["max_roi"],
                memory["rebuy_roi"],
                memory["memory_score"],
                "Imported"
            ])

        if new_rows:
            memory_ws.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"✅ Imported {len(new_rows)} memory entries to Vault_Memory.")
        else:
            print("ℹ️ No new vault tokens to import.")

    except Exception as e:
        print(f"❌ Error in run_vault_memory_importer: {e}")
