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
from roi_feedback_sync import run_roi_feedback_sync
from nova_trigger import trigger_nova_ping
from orion_voice_loop import run_orion_voice_loop  # ✅ NEW
from nova_heartbeat import log_heartbeat

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

    # Run real-time voice trigger in background
    threading.Thread(target=run_orion_voice_loop).start()  # ✅ New fast scanner

    print("🔍 Starting Watchdog...")
    run_watchdog()

    print("🧠 Running Rotation Signal Engine...")
    rotation_ws = load_presale_stream()

    scan_roi_tracking()
    log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")
    run_milestone_alerts()
    log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")

    sync_token_vault()
    print("📋 Syncing Scout Decisions → Rotation_Planner...")
    sync_rotation_planner()
    log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")

    print("📥 Syncing ROI feedback responses...")
    run_roi_feedback_sync()

    # Listen once for NovaTrigger on boot (manual pings)
    check_nova_trigger()

    trigger_nova_ping("NOVA UPDATE")

    print("⏰ Running presale scan every 60 min")
    run_presale_scorer()
    log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")

    print("💥 run_presale_scorer() BOOTED")
    print("🧠 NovaTrade system is live.")

    telegram_app.run(host="0.0.0.0", port=10000)
