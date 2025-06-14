# vault_rotation_executor.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_message, ping_webhook_debug

def run_vault_rotation_executor():
    print("🚀 Running Vault Rotation Executor...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        vault_ws = sheet.worksheet("Token_Vault")
        log_ws = sheet.worksheet("Rotation_Log")

        vault_data = vault_ws.get_all_records()
        log_data = log_ws.get_all_records()
        log_tokens = {row.get("Token", "").strip().upper() for row in log_data}

        headers = vault_ws.row_values(1)
        decision_col = headers.index("Decision") + 1
        last_reviewed_col = headers.index("Last Reviewed") + 1

        now = datetime.utcnow()
        new_rotations = 0

        for i, row in enumerate(vault_data, start=2):
            token = row.get("Token", "").strip().upper()
            decision = row.get("Decision", "").strip().upper()

            if decision != "READY TO ROTATE":
                continue
            if not token or token in log_tokens:
                continue

            score = row.get("Score", "")
            sentiment = row.get("Sentiment", "")
            market_cap = row.get("Market Cap", "")
            roi = row.get("Vault ROI", "")
            source = row.get("Source", "Vault")
            memory_tag = row.get("Memory Tag", "")
            now_str = now.strftime("%Y-%m-%d %H:%M:%S")

            log_ws.append_row([
                now_str, token, "From Vault", score, sentiment, market_cap, "", "0", "0", "N/A"
            ])
            vault_ws.update_cell(i, decision_col, "ROTATED")
            vault_ws.update_cell(i, last_reviewed_col, now.isoformat())

            send_telegram_message(
                f"📤 ${token} has been rotated out of the vault and logged to Rotation_Log.\n"
                f"ROI: {roi} | Memory: {memory_tag}"
            )

            print(f"✅ {token} rotated and logged.")
            new_rotations += 1

        print(f"✅ Vault rotation execution complete. {new_rotations} token(s) logged.")

    except Exception as e:
        print(f"❌ Error in vault_rotation_executor: {e}")
        ping_webhook_debug(f"❌ vault_rotation_executor error: {e}")
