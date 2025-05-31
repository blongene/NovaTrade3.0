from flask import Flask
from telegram_webhook import telegram_app, set_telegram_webhook
from nova_watchdog import run_watchdog
from rotation_signal_engine import scan_rotation_candidates
from roi_tracker import scan_roi_tracking
from milestone_alerts import run_milestone_alerts
from token_vault_sync import sync_token_vault
from scout_to_planner_sync import sync_rotation_planner
from presale_scorer import run_presale_scorer
from nova_trigger_watcher import check_nova_trigger
from roi_feedback_sync import run_roi_feedback_sync  # ✅ NEW
from nova_trigger import trigger_nova_ping  # ✅ Autopings (e.g., boot pings)

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import threading

# Load worksheet on boot
def load_presale_stream():
    print("⚙️ Attempting to load worksheet: Presale_Stream")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    worksheet = sheet.worksheet("Presale_Stream")
    print("✅ Loaded worksheet: Presale_Stream")
    return worksheet

# Main boot sequence
if __name__ == "__main__":
    set_telegram_webhook()

    print("📡 Orion Cloud Boot Sequence Initiated")
    print("✅ Webhook armed. Launching modules...")

    print("🔍 Starting Watchdog...")
    run_watchdog()

    print("🧠 Running Rotation Signal Engine...")
    rotation_ws = load_presale_stream()

    # ROI tracking and milestone checks
    scan_roi_tracking()
    run_milestone_alerts()

    # Sync vaults and planners
    sync_token_vault()
    print("📋 Syncing Scout Decisions → Rotation_Planner...")
    sync_rotation_planner()

    # Sync ROI feedback into review log
    print("📥 Syncing ROI feedback responses...")
    sync_roi_feedback()

    # Listen for NovaTrigger (manual A1 pings)
    check_nova_trigger()

    # Optional: trigger test boot ping
    trigger_nova_ping("NOVA UPDATE")

    print("⏰ Running presale scan every 60 min")
    run_presale_scorer()

    print("💥 run_presale_scorer() BOOTED")
    print("🧠 NovaTrade system is live.")

    # Flask webhook service
    telegram_app.run(host="0.0.0.0", port=10000)
