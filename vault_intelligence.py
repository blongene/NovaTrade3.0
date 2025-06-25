
# vault_intelligence.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_vault_intelligence():
    print("📦 Running Vault Intelligence Sync...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        # Load vault + rotation stats
        vault_ws = sheet.worksheet("Token_Vault")
        stats_ws = sheet.worksheet("Rotation_Stats")

        vault_data = vault_ws.get_all_records()
        stats_data = stats_ws.get_all_records()
        headers = stats_ws.row_values(1)

        # Determine or create Vault Tag column
        vault_col = headers.index("Vault Tag") + 1 if "Vault Tag" in headers else len(headers) + 1
        if "Vault Tag" not in headers:
            stats_ws.update_cell(1, vault_col, "Vault Tag")

        # Build sets of known tokens
        current_vault = set()
        historical_vault = set()

        for row in vault_data:
            token = str(row.get("Token", "")).strip().upper()
            status = str(row.get("Status", "")).strip().lower()
            if not token:
                continue
            historical_vault.add(token)
            if status in ["active", "held", "staked"]:
                current_vault.add(token)

        # Tag each token in Rotation_Stats
        for i, row in enumerate(stats_data, start=2):
            token = str(row.get("Token", "")).strip().upper()
            if not token:
                continue

            if token in current_vault:
                tag = "✅ Vaulted"
            elif token in historical_vault:
                tag = "🔁 Previously Vaulted"
            else:
                tag = "⚠️ Never Vaulted"

            stats_ws.update_cell(i, vault_col, tag)
            print(f"📦 {token} tagged as: {tag}")

        print("✅ Vault intelligence sync complete.")

    except Exception as e:
        print(f"❌ Vault sync error: {e}")
