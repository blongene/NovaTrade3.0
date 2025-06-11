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
from rotation_stats_sync import run_rotation_stats_sync
from rotation_feedback_engine import run_rotation_feedback_engine
from performance_dashboard import run_performance_dashboard
from rebalance_scanner import run_rebalance_scanner
from telegram_summaries import run_telegram_summaries
from rotation_memory import run_rotation_memory
from rotation_log_updater import run_rotation_log_updater
from portfolio_weight_adjuster import run_portfolio_weight_adjuster
from target_percent_updater import run_target_percent_updater
from rebuy_engine import run_undersized_rebuy
from rebuy_memory_engine import run_memory_rebuy_scan

import time
import threading
import schedule
from utils import get_gspread_client

def load_presale_stream():
    print("⚙️ Attempting to load worksheet: Presale_Stream")
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(os.getenv("SHEET_URL"))
        worksheet = sheet.worksheet("Presale_Stream")
        print("✅ Loaded worksheet: Presale_Stream")
        return worksheet
    except Exception as e:
        print(f"❌ Failed to load Presale_Stream: {e}")
        return None

def start_staking_yield_loop():
    def loop():
        from staking_yield_tracker import run_staking_yield_tracker
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

    if rotation_ws:
        scan_roi_tracking()
        time.sleep(2)
        run_milestone_alerts()
        log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")

    try:
        sync_token_vault()
    except Exception as e:
        print(f"⚠️ Vault sync error: {e}")

    time.sleep(5)
    print("📋 Syncing Scout Decisions → Rotation_Planner...")
    sync_rotation_planner()

    time.sleep(5)
    print("📥 Syncing ROI feedback responses...")
    run_roi_feedback_sync()

    time.sleep(5)
    print("📡 Running Sentiment Radar...")
    run_sentiment_radar()

    check_nova_trigger()
    time.sleep(5)
    trigger_nova_ping("NOVA UPDATE")

    print("⏰ Running presale scan every 60 min")
    run_presale_scorer()
    schedule.every(60).minutes.do(run_rotation_log_updater)
    schedule.every(60).minutes.do(run_rebalance_scanner)
    schedule.every(60).minutes.do(run_rotation_memory)
    schedule.every(3).hours.do(run_memory_rebuy_scan)

    run_stalled_asset_detector()
    check_claims()
    start_staking_yield_loop()

    time.sleep(5)
    print("🧹 Cleaning Rotation_Log ROI column...")

    time.sleep(5)
    print("📊 Syncing Rotation_Stats...")
    run_rotation_stats_sync()

    time.sleep(5)
    run_rotation_feedback_engine()

    print("📊 Running Performance Dashboard...")
    run_performance_dashboard()

    print("🔁 Running initial rebalance scan...")
    run_rebalance_scanner()

    print("📢 Running Telegram Summary Layer...")
    run_telegram_summaries()

    print("🧠 Running Rotation Memory Sync...")
    run_rotation_memory()

    time.sleep(3)
    print("🔁 Running undersized rebuy engine...")
    run_undersized_rebuy()

    time.sleep(5)
    print("♻️ Running memory-aware rebuy engine...")
    run_memory_rebuy_scan()

    time.sleep(5)
    print("🧠 Running Suggested Target Calculator...")
    run_portfolio_weight_adjuster()

    time.sleep(5)
    print("📊 Syncing Suggested % → Target %...")
    run_target_percent_updater()

    print("🧠 NovaTrade system is live.")
    print("💥 run_presale_scorer() BOOTED")
    print("🟢 Starting Flask app on port 10000...")
    telegram_app.run(host="0.0.0.0", port=10000)
