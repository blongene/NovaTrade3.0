import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime
from send_telegram import send_rotation_alert

def run_rebalance_scanner():
    print("📊 Running Rebalance Scanner...")

    try:
        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        ws = sheet.worksheet("Rebalance")  # ✅ Correct tab name
        rows = ws.get_all_records()
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for i, row in enumerate(rows, start=2):
            token = str(row.get("Token", "")).strip()
            current_pct = str(row.get("Current %", "")).replace("%", "").strip()
            target_pct = str(row.get("Target %", "")).replace("%", "").strip()
            notes = str(row.get("Notes", "")).strip()
            wallet = row.get("Wallet", "Binance_Portfolio")

            try:
                current = float(current_pct)
                target = float(target_pct)
            except:
                continue  # Skip if invalid number

            if notes.lower() in ["overweight", "undersized"]:
                message = (
                    f"⚖️ *Rebalance Suggestion: {token}*\n"
                    f"Wallet: `{wallet}`\n\n"
                    f"*Current %:* {current}%\n"
                    f"*Target %:* {target}%\n"
                    f"Status: *{notes}*\n\n"
                    f"Would you like to rebalance now?"
                )
                send_rotation_alert(token, message)
                print(f"📨 Rebalance alert sent for {token}")

    except Exception as e:
        print(f"❌ rebalance_scanner failed: {e}")
