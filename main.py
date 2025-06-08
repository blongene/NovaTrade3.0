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
from rotation_log_updater import run_rotation_log_updater
from performance_dashboard import run_performance_dashboard
from rebalance_scanner import run_rebalance_scanner
from telegram_summaries import run_telegram_summaries
from rotation_memory import run_rotation_memory
from rotation_log_cleanup import run_rotation_log_cleanup  # ✅ NEW LINE

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

    # ROI + Milestones
    scan_roi_tracking()
    run_milestone_alerts()
    log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")

    # Vault + Planner + Feedback
    try:
        sync_token_vault()
    except Exception as e:
        print(f"⚠️ Vault sync error: {e}")

    print("📋 Syncing Scout Decisions → Rotation_Planner...")
    sync_rotation_planner()

    print("📥 Syncing ROI feedback responses...")
    run_roi_feedback_sync()

    # Sentiment + Presales
    print("📡 Running Sentiment Radar...")
    run_sentiment_radar()

    check_nova_trigger
