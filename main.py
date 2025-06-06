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
from orion_voice_loop import run_orion_voice_loop
from nova_heartbeat import log_heartbeat
from stalled_asset_detector import run_stalled_asset_detector
from claim_tracker import check_claims
from sentiment_radar import run_sentiment_radar
from staking_yield_tracker import run_staking_yield_tracker
from rotation_stats_sync import run_rotation_stats_sync
from rotation_feedback_engine import run_rotation_feedback_engine
from performance_dashboard import run_performance_dashboard
from rebalance_scanner import run_rebalance_scanner
from rotation_log_updater import run_rotation_log_updater

import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import threading
import schedule

def load_presale_stream():
    print("⚙️ Attempting to load worksheet: Presale_Stream")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    worksheet = sheet.worksheet("Presale_Stream")
    print("✅ Loaded worksheet: Presale_Stream")
    return worksheet

def start_staking_yield_loop():
    def loop():
        while True:
            print("🔁 Checking staking yield...")
            run_staking_yield_tracker()
            time.sleep(21600)  # 6 hours
    threading.Thread(target=loop, daemon=True).start()

if __name__ == "__main__":
    set_telegram_webhook()
    print("📡 Orion Cloud Boot Sequence Initiated")
    print("✅ Webhook armed. Launching modules...")

    threading.Thread(target=run_orion_voice_loop).start()
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

    print("📡 Running Sentiment Radar...")
    run_sentiment_radar()

    check_nova_trigger()
    trigger_nova_ping("NOVA UPDATE")

    print("⏰ Running presale scan every 60 min")
    run_presale_scorer()
    schedule.every(60).minutes.do(run_rotation_log_updater)

    run_stalled_asset_detector()
    check_claims()
    print("💥 run_presale_scorer() BOOTED")
    print("🧠 NovaTrade system is live.")
    start_staking_yield_loop()
    run_rotation_stats_sync()
    run_rotation_feedback_engine()

    print("📊 Running Performance Dashboard...")
    run_performance_dashboard()

    print("🔁 Running initial rebalance scan...")
    run_rebalance_scanner()
    schedule.every(60).minutes.do(run_rebalance_scanner)

    telegram_app.run(host="0.0.0.0", port=10000)
