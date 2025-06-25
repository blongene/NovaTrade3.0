# portfolio_weight_adjuster.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def run_target_weight_sync():
    print("📊 Syncing Suggested % → Target %...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        ws = sheet.worksheet("Portfolio_Targets")
        data = ws.get_all_records()
        updates = 0

        for i, row in enumerate(data, start=2):
            token = str(row.get("Token", "")).strip().upper()
            suggested = str(row.get("Suggested %", "")).strip()
            target = str(row.get("Target %", "")).strip()

            try:
                if suggested and float(suggested) != float(target):
                    ws.update_acell(f"C{i}", suggested)  # Target % column
                    print(f"✅ {token} → Target % updated from {target} to {suggested}")
                    updates += 1
            except Exception as e:
                print(f"⚠️ {token} → Skipped due to invalid values: {e}")

        print(f"✅ Target % update complete. {updates} tokens adjusted.")

    except Exception as e:
        print(f"❌ Portfolio Weight Adjuster error: {e}")
