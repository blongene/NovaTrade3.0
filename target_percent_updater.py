import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def run_target_percent_updater():
    print("🔁 Updating Target % from Suggested % in Portfolio_Targets...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Portfolio_Targets")

        rows = ws.get_all_records()
        updated = 0

        for i, row in enumerate(rows, start=2):
            try:
                target_cell = f"C{i}"
                suggested_cell = f"G{i}"

                target = float(str(row.get("Target %", "")).strip() or 0)
                suggested = float(str(row.get("Suggested Target %", "")).strip() or 0)

                if suggested > 0 and abs(suggested - target) >= 0.01:
                    ws.update_acell(target_cell, suggested)
                    updated += 1
                    print(f"✅ Updated {row.get('Token', '')}: {target}% → {suggested}%")
            except Exception as inner:
                print(f"⚠️ Could not update row {i}: {inner}")

        print(f"✅ Target % update complete. {updated} tokens adjusted.")
    except Exception as e:
        print(f"❌ Target Percent Updater error: {e}")
