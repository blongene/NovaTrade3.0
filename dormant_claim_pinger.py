import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import os
from utils import send_telegram_prompt

def run_dormant_claim_alert():
    print("🔎 Scanning Claim_Tracker for unhandled claimed tokens...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Claim_Tracker")

        rows = ws.get_all_records()
        headers = ws.row_values(1)
        token_idx = headers.index("Token") + 1
        status_idx = headers.index("Status") + 1
        alerted_idx = headers.index("Last Alerted") + 1

        now = datetime.utcnow()
        threshold = timedelta(days=2)  # configurable alert threshold

        for i, row in enumerate(rows, start=2):  # start at row 2 (1-based index)
            token = row.get("Token", "").strip()
            status = row.get("Status", "").strip().upper()
            last_alerted = row.get("Last Alerted", "").strip()

            if status != "CLAIMED":
                continue

            if last_alerted:
                try:
                    last_time = datetime.strptime(last_alerted, "%Y-%m-%d %H:%M:%S")
                    if now - last_time < threshold:
                        continue  # skip recently alerted tokens
                except:
                    pass  # if unparsable, fall through and treat as needs alert

            print(f"📦 Sending prompt for claimed token: {token}")
            message = f"{token} has been marked as ✅ Claimed.\n\nHow should we handle it?"
            send_telegram_prompt(token, message, buttons=["📦 Vault It", "🔁 Rotate It", "🔕 Ignore It"], prefix="CLAIMED ACTION")
            ws.update_cell(i, alerted_idx, now.strftime("%Y-%m-%d %H:%M:%S"))

        print("✅ Dormant claim scan complete.")
    except Exception as e:
        print(f"❌ Error in dormant claim ping loop: {e}")
