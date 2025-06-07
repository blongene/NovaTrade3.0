import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
import time

def check_claims():
    try:
        print("📦 Checking claim tracker...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        tracker_ws = sheet.worksheet("Claim_Tracker")
        log_ws = sheet.worksheet("Rotation_Log")

        rows = tracker_ws.get_all_records()
        now = datetime.now()

        for i, row in enumerate(rows, start=2):
            token = str(row.get("Token", "")).strip()
            if not token:
                continue

            claimable = str(row.get("Claimable", "")).strip().upper() == "TRUE"
            claimed_str = str(row.get("Claimed?", "")).strip().lower()
            claimed = claimed_str in ["claimed", "yes", "✅"]
            unlock_date_str = row.get("Unlock Date", "")
            wallet = row.get("Wallet", "").strip()
            contract = row.get("Contract", "").strip()

            if unlock_date_str:
                try:
                    unlock_date = datetime.strptime(unlock_date_str, "%Y-%m-%d")
                    days_since_unlock = (now - unlock_date).days
                    if not claimed:
                        tracker_ws.update_acell(f"J{i}", str(days_since_unlock))
                    else:
                        tracker_ws.update_acell(f"J{i}", "")  # Clear if claimed
                except Exception as e:
                    print(f"⚠️ Could not parse unlock date for {token}: {e}")
                    tracker_ws.update_acell(f"J{i}", "❓")
            else:
                tracker_ws.update_acell(f"J{i}", "")  # Clear if no date

            # Set status based on claimability
            if claimable and not claimed:
                tracker_ws.update_acell(f"I{i}", "⚠️ Claim Now")
                print(f"⚠️ Claim reminder: {token} is unlocked and not claimed.")
            elif claimed:
                tracker_ws.update_acell(f"I{i}", "✅ Claimed")
            else:
                tracker_ws.update_acell(f"I{i}", "🕒 Pending")

            # If claimed, log it into Rotation_Log if not already there
            if claimed:
                log_data = log_ws.get_all_records()
                exists = any(str(entry["Token"]).strip().upper() == token.upper() for entry in log_data)
                if not exists:
                    print(f"✅ Logging claimed token {token} to Rotation_Log...")
                    new_row = [
                        now.strftime("%Y-%m-%d %H:%M:%S"),
                        token,
                        "Active",
                        "", "", "", "",  # Score, Sentiment, Market Cap, Scout URL
                        "100%",          # Allocation
                        "0",             # Days Held
                        "0",             # Follow-up ROI
                        "", "", "",      # Staking Yield, Contract, Initial Claimed
                        now.strftime("%Y-%m-%d %H:%M:%S"),  # Last Checked
                        "✅ Healthy"
                    ]
                    log_ws.append_row(new_row)

            time.sleep(1.5)  # Throttle to stay under quota

        print("✅ Claim tracker complete.")
    except Exception as e:
        print(f"❌ Claim tracker error: {e}")
