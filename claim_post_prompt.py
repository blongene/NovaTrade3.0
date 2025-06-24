import gspread
import os
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from utils import send_telegram_prompt

def run_claim_decision_prompt():
    try:
        print("🔍 Scanning for newly claimed tokens...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        claim_ws = sheet.worksheet("Claim_Tracker")
        scout_ws = sheet.worksheet("Scout Decisions")

        claimed_rows = claim_ws.get_all_records()
        decisions = scout_ws.get_all_records()
        decided_tokens = [r["Token"].strip().upper() for r in decisions if r.get("Decision", "").strip().upper() in ["VAULT", "ROTATE", "IGNORE"]]

        for i, row in enumerate(claimed_rows, start=2):
            token = row.get("Token", "").strip().upper()
            status = row.get("Status", "").strip().upper()

            if not token or token in decided_tokens:
                continue

            if "CLAIMED" in status:
                message = f"*{token}* has just been marked as ✅ *Claimed*.
What would you like to do next?"
                send_telegram_prompt(
                    token=token,
                    message=message,
                    buttons=["📦 Vault It", "🔁 Rotate It", "🔕 Ignore"],
                    prefix="CLAIMED ACTION"
                )
                print(f"📨 Prompt sent for {token}")
    except Exception as e:
        print(f"❌ Error in run_claim_decision_prompt: {e}")
