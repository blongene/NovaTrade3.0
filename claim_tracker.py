# claim_tracker.py
import os
import gspread
from datetime import datetime
from web3 import Web3
from oauth2client.service_account import ServiceAccountCredentials
from nova_heartbeat import log_heartbeat
from nova_trigger import trigger_nova_ping

# Wallets
METAMASK_WALLET = "0x980032AAB743379a99C4Fd18A4538c8A5DCF47d6"
BEST_WALLET = "0x71197A977c905e54b159D8154a69c6948e3Fd880"

# Connect to Ethereum RPC
web3 = Web3(Web3.HTTPProvider(os.getenv("WEB3_PROVIDER")))  # Example: Alchemy/Infura URL

def check_claims():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.environ["SHEET_URL"])
        ws = sheet.worksheet("Claim_Tracker")

        headers = ws.row_values(1)
        token_col = headers.index("Token") + 1
        source_col = headers.index("Source") + 1
        unlock_col = headers.index("Unlock Date") + 1
        claimed_col = headers.index("Claimed?") + 1
        status_col = headers.index("Status") + 1
        days_col = headers.index("Days Since Unlock") + 1

        rows = ws.get_all_values()[1:]  # skip header
        flagged = []

        for i, row in enumerate(rows, start=2):
            token = row[token_col - 1].strip()
            source = row[source_col - 1].strip()
            unlock_date = row[unlock_col - 1].strip()
            claimed = row[claimed_col - 1].strip()

            # Skip if already claimed manually
            if "✅" in claimed:
                ws.update_cell(i, status_col, "✅ Claimed")
                continue

            # Calculate days since unlock
            if unlock_date:
                try:
                    unlock_dt = datetime.strptime(unlock_date, "%Y-%m-%d")
                    days_since = (datetime.now() - unlock_dt).days
                    ws.update_cell(i, days_col, days_since)
                except Exception as e:
                    print(f"⚠️ Invalid date on row {i}: {unlock_date}")
                    continue
            else:
                ws.update_cell(i, days_col, "")
                continue

            # Lookup wallet balance (placeholder logic — to expand per token)
            if source == "MetaMask":
                wallet = METAMASK_WALLET
            elif source == "Best Wallet":
                wallet = BEST_WALLET
            else:
                continue

            # [NOTE] Replace below with actual token contract logic if needed
            # For now: assume token is unclaimed if Days > 0
            if days_since > 0:
                ws.update_cell(i, status_col, "⚠️ Claim Now")
                flagged.append(token)
            else:
                ws.update_cell(i, status_col, "⏳ Not Yet")

        if flagged:
            trigger_nova_ping("SYNC NEEDED")
            log_heartbeat("Claim Tracker", f"Claim alerts for: {', '.join(flagged)}")
        else:
            log_heartbeat("Claim Tracker", "All tokens claimed or pending")

        print("✅ Claim tracker complete.")

    except Exception as e:
        print(f"❌ Claim tracker error: {e}")
