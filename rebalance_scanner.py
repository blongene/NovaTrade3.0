import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from send_telegram import send_rotation_alert

def run_rebalance_scanner():
    print("📊 Running Rebalancer Scanner...")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        log_ws = sheet.worksheet("Rotation_Log")
        portfolio_ws = sheet.worksheet("Portfolio_Targets")

        # Build current weight snapshot
        log_data = log_ws.get_all_records()
        total_alloc = sum(float(r.get("Allocation", 0)) for r in log_data if r.get("Status") == "Active")
        current_weights = {
            r["Token"]: round(float(r["Allocation"]) / total_alloc * 100, 2)
            for r in log_data if r.get("Status") == "Active"
        }

        # Load targets
        target_data = portfolio_ws.get_all_records()
        alerts = []

        for row in target_data:
            token = row["Token"]
            target_pct = float(row["Target %"])
            actual_pct = current_weights.get(token, 0.0)
            drift = round(actual_pct - target_pct, 2)

            if abs(drift) >= 5:
                msg = f"{token} is {actual_pct}% (target {target_pct}%). Rebalance?"
                alerts.append((token, msg))

        # Telegram prompt
        for token, msg in alerts:
            send_rotation_alert(token, msg)

        if not alerts:
            print("✅ Portfolio within tolerance. No rebalance needed.")
        else:
            print(f"⚠️ {len(alerts)} drift alerts sent.")

    except Exception as e:
        print(f"❌ Rebalance scanner failed: {e}")
