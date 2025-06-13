import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import time

def run_vault_roi_tracker():
    print("📦 Running Vault ROI Tracker...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        vault_ws = sheet.worksheet("Token_Vault")
        tracker_ws = sheet.worksheet("Vault_ROI_Tracker")

        vault_data = vault_ws.get_all_records()
        tracker_data = tracker_ws.get_all_records()
        existing_tokens = set((row.get("Token", ""), str(row.get("Vault ROI", ""))) for row in tracker_data)

        new_rows = []
        today = datetime.utcnow().strftime("%Y-%m-%d")

        for row in vault_data:
            token = row.get("Token", "").strip().upper()
            roi = str(row.get("Vault ROI", "")).strip()
            tag = row.get("Memory Tag", "").strip()
            days = row.get("Days Held", "")

            if token and roi and (token, roi) not in existing_tokens:
                new_rows.append([today, token, roi, tag, days])
                print(f"📝 Logging vault ROI for {token}: {roi}")

        if new_rows:
            tracker_ws.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"✅ Vault ROI Tracker updated with {len(new_rows)} new row(s).")
        else:
            print("ℹ️ No new vault entries to track.")

    except Exception as e:
        print(f"❌ vault_roi_tracker error: {e}")
