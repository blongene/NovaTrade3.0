# vault_growth_sync.py

import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import get_gspread_client, ping_webhook_debug

def run_vault_growth_sync():
    try:
        print("📦 Syncing Vault ROI + Memory Stats...")

        client = get_gspread_client()
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        vault_ws = sheet.worksheet("Token_Vault")
        log_ws = sheet.worksheet("Rotation_Log")
        stats_ws = sheet.worksheet("Rotation_Stats")

        vault_data = vault_ws.get_all_records()
        log_data = log_ws.get_all_records()
        stats_data = stats_ws.get_all_records()

        headers = vault_ws.row_values(1)
        roi_col = headers.index("Vault ROI") + 1
        days_col = headers.index("Days Held") + 1
        tag_col = headers.index("Memory Tag") + 1
        score_col = headers.index("Memory Score") + 1

        updated = 0

        for i, row in enumerate(vault_data, start=2):
            token = row.get("Token", "").strip().upper()
            if not token:
                continue

            log_match = next((r for r in log_data if r.get("Token", "").strip().upper() == token), None)
            stat_match = next((r for r in stats_data if r.get("Token", "").strip().upper() == token), None)

            roi = log_match.get("Follow-up ROI", "") if log_match else ""
            days = log_match.get("Days Held", "") if log_match else ""
            tag = stat_match.get("Memory Tag", "") if stat_match else ""
            score = stat_match.get("Memory Score", "") if stat_match else ""

            vault_ws.update_cell(i, roi_col, str(roi))
            vault_ws.update_cell(i, days_col, str(days))
            vault_ws.update_cell(i, tag_col, str(tag))
            vault_ws.update_cell(i, score_col, str(score))
            updated += 1

        print(f"✅ Vault Growth sync complete. {updated} rows updated.")

    except Exception as e:
        print(f"❌ vault_growth_sync error: {e}")
        ping_webhook_debug(f"❌ Vault Growth Sync Error: {e}")
