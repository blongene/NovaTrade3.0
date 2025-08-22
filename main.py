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
from sentiment_summary import run_sentiment_summary
from rotation_stats_sync import run_rotation_stats_sync
from rotation_feedback_engine import run_rotation_feedback_engine
from performance_dashboard import run_performance_dashboard
from rebalance_scanner import run_rebalance_scanner
from telegram_summaries import run_telegram_summary
from rotation_memory import run_rotation_memory
from rotation_log_updater import run_rotation_log_updater
from portfolio_weight_adjuster import run_portfolio_weight_adjuster
from target_percent_updater import run_target_percent_updater
from rebuy_engine import run_undersized_rebuy
from rebuy_memory_engine import run_memory_rebuy_scan
from rebuy_roi_tracker import run_rebuy_roi_tracker
from rotation_memory_scoring import run_memory_scoring
from vault_intelligence import run_vault_intelligence
from vault_to_stats_sync import run_vault_to_stats_sync
from vault_alerts_phase15d import run_vault_alerts
from vault_growth_sync import run_vault_growth_sync
from vault_roi_tracker import run_vault_roi_tracker
from vault_review_alerts import run_vault_review_alerts
from utils import get_gspread_client, send_telegram_message
from vault_rotation_scanner import run_vault_rotation_scanner
from vault_rotation_executor import run_vault_rotation_executor
from wallet_monitor import run_wallet_monitor
from unlock_horizon_alerts import run_unlock_horizon_alerts
from top_token_summary import run_top_token_summary
from auto_confirm_planner import run_auto_confirm_planner
from memory_weight_sync import run_memory_weight_sync
from sentiment_trigger_engine import run_sentiment_trigger_engine
from roi_threshold_validator import run_roi_threshold_validator
from sentiment_alerts import run_sentiment_alerts
from rebuy_weight_calculator import run_rebuy_weight_calculator
from memory_score_sync import run_memory_score_sync
from claim_post_prompt import run_claim_decision_prompt
from dormant_claim_pinger import run_dormant_claim_alert
from vault_rotation_gatekeeper import gate_vault_rotation
from total_memory_score_sync import sync_total_memory_score
from vault_memory_evaluator import evaluate_vault_memory
from vault_memory_importer import run_vault_memory_importer

import os, time, threading, schedule, gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Boot Configuration ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
RENDER_WEBHOOK_URL = os.getenv("RENDER_WEBHOOK_URL")

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

def start_flask_app():
    print("🟢 Starting Flask app on port 10000...")
    telegram_app.run(host="0.0.0.0", port=10000)

def run_scheduler_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)

def threaded(func):
    threading.Thread(target=func).start()
    
if __name__ == "__main__":
    set_telegram_webhook()
    threading.Thread(target=start_flask_app).start()
    print("📱 Orion Cloud Boot Sequence Initiated")
    print("✅ Webhook armed. Launching modules...")
    threading.Thread(target=run_orion_voice_loop).start()

    print("🔍 Starting Watchdog...")
    run_watchdog()

    print("🧠 Running Rotation Signal Engine...")
    rotation_ws = load_presale_stream()

    if rotation_ws:
        scan_roi_tracking()
        time.sleep(5)
        try:
            run_milestone_alerts()
        except Exception as e:
            print(f"❌ Error in run_milestone_alerts: {e}")
        time.sleep(5)
        log_heartbeat("ROI Tracker", "Updated Days Held for 4 tokens")

    try: sync_token_vault()
    except Exception as e: print(f"⚠️ Vault sync error: {e}")

    time.sleep(10)
    try: run_top_token_summary()
    except Exception as e: print(f"❌ Error in run_top_token_summary: {e}")

    time.sleep(10)
    run_vault_intelligence()
    time.sleep(10)
    print("🚀 Executing any pending vault rotations...")
    gate_vault_rotation("MIND")
    run_vault_rotation_executor()
    time.sleep(10)
    print("📋 Syncing Scout Decisions → Rotation_Planner...")
    sync_rotation_planner()
    time.sleep(10)
    print("📅 Syncing ROI feedback responses...")
    run_roi_feedback_sync()

    time.sleep(10)
    print("📰 Running Sentiment Radar (1x boot pass only)...")
    try:
        run_sentiment_radar()
    except Exception as e:
        print(f"⚠️ Radar scan skipped due to error: {e}")

    check_nova_trigger()
    time.sleep(10)
    trigger_nova_ping("NOVA UPDATE")
    check_claims()
    run_claim_decision_prompt()
    
    if rotation_ws:
        print("⏰ Running presale scan every 60 min")
        run_presale_scorer()
    else:
        print("⛔️ Presale_Stream unavailable — presale scan skipped")

    schedule.every(60).minutes.do(run_rotation_log_updater)
    schedule.every(60).minutes.do(run_rebalance_scanner)
    schedule.every(60).minutes.do(run_rotation_memory)
    schedule.every(6).hours.do(run_sentiment_radar)
    schedule.every(3).hours.do(run_memory_rebuy_scan)
    schedule.every(3).hours.do(run_sentiment_summary)
    schedule.every().day.at("02:00").do(run_vault_roi_tracker)
    schedule.every().day.at("09:15").do(run_vault_rotation_scanner)
    schedule.every().day.at("09:25").do(run_vault_rotation_executor)
    schedule.every().day.at("09:45").do(run_wallet_monitor)
    schedule.every().day.at("13:00").do(run_sentiment_alerts)
    schedule.every().day.at("01:30").do(run_top_token_summary)
    schedule.every().day.at("01:00").do(run_roi_threshold_validator)
    schedule.every().day.at("12:45").do(run_rebuy_roi_tracker)
    schedule.every().day.at("01:10").do(run_rebuy_weight_calculator)
    schedule.every().day.at("01:15").do(run_memory_score_sync)
    schedule.every(6).hours.do(run_dormant_claim_alert)
    
    threading.Thread(target=run_scheduler_loop, daemon=True).start()
    run_stalled_asset_detector()
    time.sleep(10)
    check_claims()
    time.sleep(10)
    start_staking_yield_loop()
    time.sleep(10)

    print("🪚 Cleaning Rotation_Log ROI column...")
    time.sleep(10)

    threaded(run_rotation_stats_sync)
    time.sleep(10)

    threaded(run_memory_weight_sync)
    time.sleep(10)
    
    print("🧠 Calculating Total Memory Score...")
    run_memory_score_sync()
    time.sleep(10)
    
    threaded(run_rebuy_roi_tracker)
    time.sleep(10)

    try: run_rotation_feedback_engine()
    except Exception as e: print(f"❌ Error in run_rotation_feedback_engine: {e}")

    time.sleep(10)
    try:
        print("📊 Running Performance Dashboard...")
        run_performance_dashboard()
    except Exception as e:
        print(f"⚠️ Skipped Dashboard due to quota: {e}")

    print("🔁 Running initial rebalance scan...")
    run_rebalance_scanner()

    print("📢 Running Telegram Summary Layer...")
    run_telegram_summary()

    print("🧠 Running Rotation Memory Sync...")
    run_rotation_memory()
    time.sleep(10)
    print("🔁 Running undersized rebuy engine...")
    run_undersized_rebuy()
    time.sleep(10)
    print("♻️ Running memory-aware rebuy engine...")
    run_memory_rebuy_scan()
    time.sleep(10)
    print("🧠 Calculating Rebuy Weights...")
    run_rebuy_weight_calculator()
    
    time.sleep(10)
    print("🚨 Running Sentiment-Triggered Rebuy Scan...")
    try:
        run_sentiment_trigger_engine()
    except Exception as e:
        print(f"❌ Error in run_sentiment_trigger_engine: {e}")

    time.sleep(10)
    run_memory_scoring()
    time.sleep(10)
    print("🧠 Running Suggested Target Calculator...")
    run_portfolio_weight_adjuster()
    time.sleep(10)
    print("📊 Syncing Suggested % → Target %...")
    run_target_percent_updater()
    time.sleep(15)
    sync_total_memory_score()
    print("📊 Syncing Vault Tags → Rotation_Stats...")
    threaded(run_vault_to_stats_sync)
    time.sleep(10)

    try: run_vault_alerts()
    except Exception as e: print(f"❌ Error in run_vault_alerts: {e}")

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

    print("🔁 Scanning vaults for decay...")
    try:
        run_vault_rotation_scanner()
    except Exception as e:
        print(f"❌ Error in run_vault_rotation_scanner: {e}")

# ✅ Conditionally run Binance executor if allowed
if os.getenv("ENABLE_CLOUD_BINANCE", "false").lower() == "true":
    try:
        from rotation_binance_executor import run_rotation_binance_executor
        run_rotation_binance_executor()
    except Exception as e:
        print(f"⚠️ Skipping Binance executor: {e}")
else:
    print("⚠️ Binance executor skipped (ENABLE_CLOUD_BINANCE is false)")
    print("📋 Running Auto-Confirm Planner...")
    run_auto_confirm_planner()
    print("✅ Auto-confirm check complete.")
    evaluate_vault_memory()
    run_vault_memory_importer()
    
    time.sleep(10)
    run_unlock_horizon_alerts()
    print("💥 run_presale_scorer() BOOTED")
    send_telegram_message("🟢 NovaTrade system booted and live.")
    print("🧠 NovaTrade system is live.")



import os
from telegram_webhook import telegram_app, set_telegram_webhook
from nova_watchdog import start_watchdog

if __name__ == "__main__":
    print("📡 Orion Cloud Boot Sequence Initiated")
    set_telegram_webhook()
    start_watchdog()
    telegram_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

