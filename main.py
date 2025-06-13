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
from rotation_memory_scoring import run_memory_scoring
from vault_intelligence import run_vault_intelligence
from vault_to_stats_sync import run_vault_to_stats_sync
from vault_alerts_phase15d import run_vault_alerts
from vault_growth_sync import run_vault_growth_sync
from vault_roi_tracker import run_vault_roi_tracker
from vault_review_alerts import run_vault_review_alerts

import os
import time
import threading
import schedule
import gspread
from utils import get_gspread_client
from oauth2client.service_account import ServiceAccountCredentials

def load_presale_stream():
    print("⚙️ Attempting to load worksheet: Presale_Stream")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
        client = gspread.authorize(creds)
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
            time.sleep(21600)
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
        time.sleep(5)
        run_milestone_alerts()
        log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")

    try:
        sync_token_vault()
    except Exception as e:
        print(f"⚠️ Vault sync error: {e}")

    time.sleep(10)
    run_vault_intelligence()

    time.sleep(10)
    print("📋 Syncing Scout Decisions → Rotation_Planner...")
    sync_rotation_planner()

    time.sleep(10)
    print("📥 Syncing ROI feedback responses...")
    run_roi_feedback_sync()

    time.sleep(10)
    print("📡 Running Sentiment Radar (1x boot pass only)...")
    try:
        run_sentiment_radar()
    except Exception as e:
        print(f"⚠️ Radar scan skipped due to error: {e}")

    check_nova_trigger()
    time.sleep(10)
    trigger_nova_ping("NOVA UPDATE")

    if rotation_ws:
        print("⏰ Running presale scan every 60 min")
        run_presale_scorer()
    else:
        print("⛔️ Presale_Stream unavailable — presale scan skipped")

    schedule.every(60).minutes.do(run_rotation_log_updater)
    schedule.every(60).minutes.do(run_rebalance_scanner)
    schedule.every(60).minutes.do(run_rotation_memory)
    schedule.every(3).hours.do(run_memory_rebuy_scan)
    schedule.every().day.at("02:00").do(run_vault_roi_tracker)

    run_stalled_asset_detector()
    time.sleep(10)
    check_claims()
    time.sleep(10)
    start_staking_yield_loop()
    time.sleep(10)

    print("🧹 Cleaning Rotation_Log ROI column...")
    time.sleep(10)
    print("📊 Syncing Rotation_Stats...")
    try:
        run_rotation_stats_sync()
    except Exception as e:
        print(f"❌ Error in run_rotation_stats_sync: {e}")

    time.sleep(10)
    try:
        run_rotation_feedback_engine()
    except Exception as e:
        print(f"❌ Error in run_rotation_feedback_engine: {e}")

    try:
        time.sleep(10)
        print("📊 Running Performance Dashboard...")
        run_performance_dashboard()
    except Exception as e:
        print(f"⚠️ Skipped Dashboard due to quota: {e}")


    print("🔁 Running initial rebalance scan...")
    run_rebalance_scanner()

    print("📢 Running Telegram Summary Layer...")
    run_telegram_summaries()

    print("🧠 Running Rotation Memory Sync...")
    run_rotation_memory()

    time.sleep(10)
    print("🔁 Running undersized rebuy engine...")
    run_undersized_rebuy()

    time.sleep(10)
    print("♻️ Running memory-aware rebuy engine...")
    run_memory_rebuy_scan()

    time.sleep(10)
    run_memory_scoring()

    time.sleep(10)
    print("🧠 Running Suggested Target Calculator...")
    run_portfolio_weight_adjuster()

    time.sleep(10)
    print("📊 Syncing Suggested % → Target %...")
    run_target_percent_updater()

    time.sleep(15)
    print("📊 Syncing Vault Tags → Rotation_Stats...")
    try:
        run_vault_to_stats_sync()
    except Exception as e:
        print(f"❌ vault_to_stats_sync error: {e}")

    time.sleep(10)
    print("🔔 Running Vault Intelligence Alerts...")
    try:
        run_vault_alerts()
    except Exception as e:
        print(f"❌ Error in run_vault_alerts: {e}")

    time.sleep(10)
    print("📦 Syncing Vault ROI + Memory Stats...")
    try:
        run_vault_growth_sync()
    except Exception as e:
        print(f"❌ vault_growth_sync error: {e}")

    time.sleep(5)
    print("📈 Writing daily snapshot to Vault ROI Tracker...")
    try:
        run_vault_roi_tracker()
    except Exception as e:
        print(f"❌ Error in run_vault_roi_tracker: {e}")

    time.sleep(5)
    print("📬 Running Vault Review Alerts...")
    try:
        run_vault_review_alerts()
    except Exception as e:
        print(f"❌ Error in run_vault_review_alerts: {e}")
    
    print("🧠 NovaTrade system is live.")
    print("💥 run_presale_scorer() BOOTED")
    print("🟢 Starting Flask app on port 10000...")
    telegram_app.run(host="0.0.0.0", port=10000)
