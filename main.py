# main.py — NT3.0 boot polish (quiet + quota-safe)
import os, time, threading, schedule

from flask import Flask
from telegram_webhook import telegram_app, set_telegram_webhook

# === Import your modules ===
from nova_watchdog import run_watchdog
from rotation_signal_engine import scan_rotation_candidates  # kept if you use it elsewhere
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
from telegram_summaries import run_telegram_summaries
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

# === Utils (new polished helpers) ===
from utils import (
    get_gspread_client, send_telegram_message_dedup,  # compat
    send_boot_notice_once, send_system_online_once,
    info, warn, error, get_ws_cached, get_all_records_cached
)

# === Boot Configuration ===
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
RENDER_WEBHOOK_URL = os.getenv("RENDER_WEBHOOK_URL")
SHEET_URL = os.getenv("SHEET_URL")

def load_presale_stream():
    info("Attempting to load worksheet: Presale_Stream")
    try:
        client = get_gspread_client()
        sheet = client.open_by_url(SHEET_URL)
        worksheet = sheet.worksheet("Presale_Stream")
        info("Loaded worksheet: Presale_Stream")
        return worksheet
    except Exception as e:
        warn(f"Failed to load Presale_Stream: {e}")
        return None

def start_staking_yield_loop():
    def loop():
        from staking_yield_tracker import run_staking_yield_tracker
        while True:
            info("Checking staking yield…")
            try:
                run_staking_yield_tracker()
            except Exception as e:
                warn(f"staking_yield_tracker error: {e}")
            time.sleep(21600)  # 6h
    threading.Thread(target=loop, daemon=True).start()

def start_flask_app():
    info("Starting Flask app on port 10000…")
    telegram_app.run(host="0.0.0.0", port=10000)

def run_scheduler_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)

def threaded(fn, *args, **kwargs):
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return t

def _safe(label, fn, *args, **kwargs):
    try:
        info(f"▶ {label}")
        return fn(*args, **kwargs)
    except Exception as e:
        error(f"{label} failed: {e}")

if __name__ == "__main__":
    # Webhook + boot notices
    set_telegram_webhook()
    send_boot_notice_once("🟢 NovaTrade system booted and live.")

    # Core services
    threaded(start_flask_app)
    time.sleep(1.0)

    threaded(run_orion_voice_loop)
    time.sleep(0.5)

    _safe("Starting Watchdog", run_watchdog)
    time.sleep(0.8)

    rotation_ws = load_presale_stream()
    time.sleep(0.5)

    # —— serialize the heavy first minute to avoid 429s ——
    if rotation_ws:
        _safe("ROI tracker (boot pass)", scan_roi_tracking);      time.sleep(0.9)
        _safe("Milestone alerts (boot pass)", run_milestone_alerts); time.sleep(0.9)
        log_heartbeat("ROI Tracker", "Boot pass complete")

    _safe("Vault sync",                    sync_token_vault);              time.sleep(0.7)
    _safe("Top token summary",             run_top_token_summary);         time.sleep(0.7)
    _safe("Vault intelligence",            run_vault_intelligence);        time.sleep(0.7)
    _safe("Vault rotation executor",       run_vault_rotation_executor);   time.sleep(0.7)
    _safe("Scout→Planner sync",            sync_rotation_planner);         time.sleep(0.7)
    _safe("ROI feedback sync",             run_roi_feedback_sync);         time.sleep(0.7)

    # One-shot radar (errors fine; quota-aware)
    _safe("Sentiment Radar (boot pass)",   run_sentiment_radar);           time.sleep(0.7)

    check_nova_trigger()
    time.sleep(0.5)
    try:
        trigger_nova_ping("NOVA UPDATE")
    except Exception as e:
        warn(f"Nova ping skipped: {e}")

    send_system_online_once()

    # —— Schedules (light staggering) ——
    # frequent
    schedule.every(60).minutes.do(run_rotation_log_updater)
    schedule.every(60).minutes.do(run_rebalance_scanner)
    schedule.every(60).minutes.do(run_rotation_memory)
    schedule.every(6).hours.do(run_sentiment_radar)
    schedule.every(3).hours.do(run_memory_rebuy_scan)
    schedule.every(3).hours.do(run_sentiment_summary)

    # daily cadence (spread by minutes)
    schedule.every().day.at("01:00").do(run_roi_threshold_validator)
    schedule.every().day.at("01:10").do(run_rebuy_weight_calculator)
    schedule.every().day.at("01:15").do(run_memory_score_sync)
    schedule.every().day.at("01:30").do(run_top_token_summary)
    schedule.every().day.at("02:00").do(run_vault_roi_tracker)
    schedule.every().day.at("09:15").do(run_vault_rotation_scanner)
    schedule.every().day.at("09:25").do(run_vault_rotation_executor)
    schedule.every().day.at("09:45").do(run_wallet_monitor)
    schedule.every().day.at("12:45").do(run_rebuy_roi_tracker)
    schedule.every().day.at("13:00").do(run_sentiment_alerts)

    # background loops & once-offs
    threaded(run_scheduler_loop)
    _safe("Stalled asset detector (boot pass)", run_stalled_asset_detector); time.sleep(0.6)
    _safe("Claim tracker (boot pass)",          check_claims);               time.sleep(0.6)
    start_staking_yield_loop();                                                time.sleep(0.6)

    # stats & memory syncs (spaced)
    info("Cleaning Rotation_Log ROI column…");                                 time.sleep(0.6)
    threaded(run_rotation_stats_sync);                                          time.sleep(0.6)
    threaded(run_memory_weight_sync);                                           time.sleep(0.6)
    _safe("Total memory score", run_memory_score_sync);                         time.sleep(0.6)
    threaded(run_rebuy_roi_tracker);                                            time.sleep(0.6)

    _safe("Rotation feedback engine", run_rotation_feedback_engine);            time.sleep(0.6)

    try:
        info("Running Performance Dashboard…")
        run_performance_dashboard()
    except Exception as e:
        warn(f"Dashboard skipped (quota or parse): {e}")

    info("Running initial rebalance scan…")
    run_rebalance_scanner()

    info("Running Telegram Summary Layer…")
    run_telegram_summaries()

    info("Running Rotation Memory Sync…")
    run_rotation_memory();                                                      time.sleep(0.6)

    info("Running undersized rebuy engine…")
    run_undersized_rebuy();                                                     time.sleep(0.6)

    info("Running memory-aware rebuy engine…")
    run_memory_rebuy_scan();                                                    time.sleep(0.6)

    info("Calculating Rebuy Weights…")
    run_rebuy_weight_calculator();                                              time.sleep(0.6)

    try:
        info("Sentiment-Triggered Rebuy Scan…")
        run_sentiment_trigger_engine()
    except Exception as e:
        warn(f"Sentiment trigger engine error: {e}")

    run_memory_scoring();                                                       time.sleep(0.6)
    info("Running Suggested Target Calculator…")
    run_portfolio_weight_adjuster();                                            time.sleep(0.6)
    info("Syncing Suggested % → Target %…")
    run_target_percent_updater();                                               time.sleep(0.8)

    info("Syncing Vault Tags → Rotation_Stats…")
    threaded(run_vault_to_stats_sync);                                          time.sleep(0.6)

    try:
        run_vault_alerts()
    except Exception as e:
        warn(f"Vault alerts error: {e}")

    try:
        info("Vault ROI + Memory Stats sync…")
        run_vault_growth_sync()
    except Exception as e:
        warn(f"vault_growth_sync error: {e}")

    try:
        info("Writing daily snapshot to Vault ROI Tracker…")
        run_vault_roi_tracker()
    except Exception as e:
        warn(f"Vault ROI tracker error: {e}")

    try:
        info("Running Vault Review Alerts…")
        run_vault_review_alerts()
    except Exception as e:
        warn(f"Vault review alerts error: {e}")

    try:
        info("Scanning vaults for decay…")
        run_vault_rotation_scanner()
    except Exception as e:
        warn(f"Vault rotation scanner error: {e}")

    info("Auto-Confirm Planner…")
    run_auto_confirm_planner()
    info("Auto-confirm check complete.")

    info("Unlock horizon alerts…")
    run_unlock_horizon_alerts()

    # Final boot ping (de-duped already)
    send_telegram_message_dedup("✅ NovaTrade boot sequence complete.", key="boot_done")
