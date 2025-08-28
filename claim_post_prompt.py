# claim_post_prompt.py

import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from utils import send_telegram_prompt, get_records_cached


def run_claim_decision_prompt():
    try:
        print("🔍 Scanning for newly claimed tokens...")

        # Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))

        claim_ws = sheet.worksheet("Claim_Tracker")
        scout_ws = sheet.worksheet("Scout Decisions")

        # ✅ Use our utils wrapper, not direct ws.get_records_cached
        claimed_rows = get_records_cached("Claim_Tracker", ttl_s=180)
        decisions = get_records_cached("Scout Decisions", ttl_s=180)

        decided_tokens = [
            (r.get("Token") or "").strip().upper()
            for r in decisions
            if (r.get("Decision") or "").strip().upper() in ["VAULT", "ROTATE", "IGNORE"]
        ]

        for i, row in enumerate(claimed_rows, start=2):
            token = (row.get("Token") or "").strip().upper()
            status = (row.get("Status") or "").strip().upper()

            if not token or token in decided_tokens:
                continue

            if "CLAIMED" in status:
                message = f"*{token}* has just been marked as ✅ *Claimed*.\nWhat would you like to do next?"
                send_telegram_prompt(
                    token=token,
                    message=message,
                    buttons=["📦 Vault It", "🔁 Rotate It", "🔕 Ignore"],
                    prefix="CLAIMED ACTION"
                )
                print(f"📨 Prompt sent for {token}")

    except Exception as e:
        print(f"❌ Error in run_claim_decision_prompt: {e}")
