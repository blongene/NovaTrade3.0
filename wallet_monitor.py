# wallet_monitor.py

import os
import requests
from utils import send_telegram_message, get_gspread_client
from datetime import datetime

ZAPPER_API_KEY = os.getenv("ZAPPER_API_KEY")
METAMASK_ADDRESS = "0x980032AAB743379a99C4Fd18A4538c8A5DCF47d6"
BESTWALLET_ADDRESS = "0x71197A977c905e54b159D8154a69c6948e3Fd880"
SHEET_URL = os.getenv("SHEET_URL")

def fetch_wallet_tokens(address):
    url = f"https://api.zapper.xyz/v2/balances/tokens?addresses[]={address}&api_key={ZAPPER_API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch wallet tokens: {response.status_code} - {response.text}")
            return []
        data = response.json()
        tokens = []
        for account in data.get(address.lower(), {}).get("products", []):
            for asset in account.get("assets", []):
                symbol = asset.get("symbol", "").upper()
                balance = float(asset.get("balance", 0))
                if balance > 0 and symbol:
                    tokens.append(symbol)
        return list(set(tokens))
    except Exception as e:
        print(f"❌ Error fetching tokens from Zapper: {e}")
        return []

def run_wallet_monitor():
    print("🔍 Running Wallet Monitor...")

    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SHEET_URL)
        claim_ws = sheet.worksheet("Claim_Tracker")
        decisions_ws = sheet.worksheet("Scout Decisions")

        claim_data = claim_ws.get_all_records()
        decision_data = decisions_ws.get_all_records()

        claimed_tokens = {
            row["Token"].strip().upper()
            for row in claim_data
            if row.get("Claimed?", "").strip().lower() == "claimed"
        }

        pending_claims = {
            row["Token"].strip().upper()
            for row in claim_data
            if row.get("Claimed?", "").strip().lower() != "claimed"
        }

        all_approved = {
            row["Token"].strip().upper()
            for row in decision_data
            if row.get("Decision", "").strip().upper() == "YES"
        }

        all_wallet_tokens = set()
        all_wallet_tokens.update(fetch_wallet_tokens(METAMASK_ADDRESS))
        all_wallet_tokens.update(fetch_wallet_tokens(BESTWALLET_ADDRESS))

        print(f"🧾 Wallet Tokens: {all_wallet_tokens}")
        print(f"📋 Pending Claim Tokens: {pending_claims}")

        # Notify if wallet contains tokens not yet marked as claimed
        unknown_arrivals = [
            token for token in all_wallet_tokens
            if token in all_approved and token not in claimed_tokens
        ]

        for token in unknown_arrivals:
            msg = (
                f"⚠️ *{token}* has arrived in your wallet,\n"
                f"but is *not marked as claimed* in the sheet.\n\n"
                f"Would you like to mark it as claimed?"
            )
            send_telegram_message(msg)
            print(f"🔔 Alert sent for token: {token}")

            # Auto-mark as "Resolved" in Status column
            for i, row in enumerate(claim_data, start=2):  # row 2 = first data row
                if row.get("Token", "").strip().upper() == token:
                    claim_ws.update_acell(f"I{i}", "Resolved")
                    print(f"✅ Status for {token} set to Resolved")

        print("✅ Wallet monitor complete.")

    except Exception as e:
        print(f"❌ Error in run_wallet_monitor: {e}")
