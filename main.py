import os
import time
from flask import Flask
from telegram_webhook import set_telegram_webhook
from presale_scorer import run_presale_scorer
from token_vault_sync import sync_token_vault
from scout_to_planner_sync import sync_rotation_log, sync_rotation_planner
from roi_tracker import update_roi_days
from milestone_alerts import run_milestone_alerts
from rotation_signal_engine import scan_rotation_candidates
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Authenticate Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
client = gspread.authorize(creds)

sheet = client.open_by_url(os.getenv("SHEET_URL"))
scout_ws = sheet.worksheet("Presale_Stream")
print("✅ Loaded worksheet: Presale_Stream")

# Initialize Flask app
app = Flask(__name__)
set_telegram_webhook()

# 🔁 Launch all active modules
print("📡 Orion Cloud Boot Sequence Initiated")
print("✅ Webhook armed. Launching modules...")

print("🔍 Starting Watchdog...")
rotation_ws = sheet.worksheet("Rotation_Stats")
rotation_rows = rotation_ws.get_all_records()

print("🧠 Running Rotation Signal Engine...")
scan_rotation_candidates(rotation_rows)

print("📈 Checking for ROI milestone follow-ups...")
update_roi_days(rotation_ws)
run_milestone_alerts(rotation_ws)

print("✅ Token Vault synced with latest Scout Decisions.")
sync_token_vault()

print("🧲 Syncing Confirmed Tokens to Rotation_Log...")
sync_rotation_log()

print("📋 Syncing Scout Decisions → Rotation_Planner...")
sync_rotation_planner()

print("📈 Checking for ROI milestone follow-ups...")
update_roi_days(rotation_ws)
run_milestone_alerts(rotation_ws)

print("⏰ Running presale scan every 60 min")
run_presale_scorer()

print("💥 run_presale_scorer() BOOTED")
print("🧠 NovaTrade system is live.")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=10000)
