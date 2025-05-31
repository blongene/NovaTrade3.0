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
from nova_trigger_sender import trigger_nova_ping  # 🆕 Add this line
from roi_feedback_sync import sync_roi_feedback

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import threading
from nova_trigger_listener import listen_for_nova_trigger  # already present

# Load worksheet (e.g. Presale_Stream) on boot
def load_presale_stream():
    print("⚙️ Attempting to load worksheet: Presale_Stream")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    worksheet = sheet.worksheet("Presale_Stream")
    print("✅ Loaded worksheet: Presale_Stream")
    return worksheet

# Launch Webhook + Background Modules
if __name__ == "__main__":
    set_telegram_webhook()

    print("📡 Orion Cloud Boot Sequence Initiated")
    print("✅ Webhook armed. Launching modules...")

    print("🔍 Starting Watchdog...")
    run_watchdog()

    print("🧠 Running Rotation Signal Engine...")
    rotation_ws = load_presale_stream()  # Used only for boot validation

    # Run rotation performance + follow-up logic
    scan_roi_tracking()
    run_milestone_alerts()

    # Sync decision logs + planner entries
    sync_token_vault()
    print("📋 Syncing Scout Decisions → Rotation_Planner...")
    sync_rotation_planner()

    # Check if NovaTrigger wants attention
    check_nova_trigger()

    print("⏰ Running presale scan every 60 min")
    run_presale_scorer()

    print("💥 run_presale_scorer() BOOTED")
    print("🧠 NovaTrade system is live.")

    # 🔔 Send autonomous test ping after full system boot
    trigger_nova_ping("SOS")

    # Start background listener thread
    threading.Thread(target=listen_for_nova_trigger, daemon=True).start()

    # Start Flask app
    telegram_app.run(host="0.0.0.0", port=10000)

    # Sync user feedback from ROI log
    sync_roi_feedback()
