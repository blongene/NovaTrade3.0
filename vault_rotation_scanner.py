# vault_rotation_scanner.py

import os
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from utils import send_telegram_prompt, ping_webhook_debug

def run_vault_rotation_scanner():
    print("🔁 Scanning Vault for Rotation Candidates...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        vault_ws = sheet.worksheet("Token_Vault")
        vault_data = vault_ws.get_all_records()
        headers = vault_ws.row_values(1)

        sentiment_col = headers.index("Sentiment") + 1
        score_col = headers.index("Memory Score") + 1
        last_reviewed_col = headers.index("Last Reviewed") + 1
        decision_col = headers.index("Decision") + 1

        now = datetime.utcnow()
        sent_count = 0

        for i, row in enumerate(vault_data, start=2):  # i=2 for header offset
            token = row.get("Token", "").strip().upper()
            status = row.get("Status", "").strip().lower()
            sentiment = row.get("Sentiment", "")
            memory_score = row.get("Memory Score", "")
            last_reviewed = row.get("Last Reviewed", "")

            if status not in ["active", "staked"]:
                continue

            try:
                s_val = float(sentiment)
                m_val = float(memory_score)
            except:
                continue

            try:
                if last_reviewed:
                    dt = datetime.strptime(last_reviewed, "%Y-%m-%dT%H:%M:%S")
                    if (now - dt).days < 3:
                        continue  # recently reviewed
            except:
                pass  # proceed if date is missing or corrupt

            if s_val < 10 or m_val <= 0:
                prompt = (
                    f"🔐 Vault Rotation Review\n\n"
                    f"{token} is currently vaulted, but may be underperforming:\n"
                    f"📉 Sentiment: {s_val}\n"
                    f"🧠 Memory Score: {m_val}\n\n"
                    f"Would you like to keep it vaulted?"
                )
                send_telegram_prompt(token, prompt, buttons=["YES", "NO"], prefix="VAULT ROTATION")
                vault_ws.update_cell(i, last_reviewed_col, now.isoformat())
                sent_count += 1

        print(f"✅ Vault rotation check complete. {sent_count} prompt(s) sent.")

    except Exception as e:
        print(f"❌ Error in run_vault_rotation_scanner: {e}")
        ping_webhook_debug(f"❌ vault_rotation_scanner error: {e}")
