
# vault_to_stats_sync.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import throttle_retry
from utils import get_records_cached

@throttle_retry()
def run_vault_to_stats_sync():
    print("📊 Syncing Vault Tags → Rotation_Stats...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        vault_ws = sheet.worksheet("Token_Vault")
        stats_ws = sheet.worksheet("Rotation_Stats")

        vault_data = vault_ws.get_records_cached("Token_Vault", ttl_s=180)  # 3‑minute cache
        stats_data = stats_ws.get_records_cached("Rotation_Stats", ttl_s=180)  # 3‑minute cache

        vault_dict = {row["Token"].strip().upper(): str(row.get("Vault Tag", "")).strip() for row in vault_data if row.get("Token")}
        headers = stats_ws.row_values(1)
        vault_tag_col = headers.index("Vault Tag") + 1 if "Vault Tag" in headers else len(headers) + 1

        if "Vault Tag" not in headers:
            stats_ws.update_cell(1, vault_tag_col, "Vault Tag")

        updates = 0
        for i, row in enumerate(stats_data, start=2):
            token = str(row.get("Token", "")).strip().upper()
            tag = vault_dict.get(token, "")
            if tag and str(row.get("Vault Tag", "")).strip() != tag:
                stats_ws.update_cell(i, vault_tag_col, tag)
                updates += 1
                print(f"✅ {token} → {tag}")

        print(f"🔁 Vault Tag sync complete. {updates} rows updated.")

    except Exception as e:
        print(f"❌ vault_to_stats_sync error: {e}")
