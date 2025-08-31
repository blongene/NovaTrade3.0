# main.py — NovaTrade 3.0 (Phase-1 Polish)
# Boot is serialized + gentle; all Sheets IO goes through utils.
from logging_setup import init_logging
log = init_logging()
log.info("📡 Orion Cloud Boot Sequence Initiated")

import os, time, threading, schedule, random, hashlib, json
from flask import Flask

# Telegram webhook / Flask app
from telegram_webhook import telegram_app, set_telegram_webhook

# Quiet, de-duped operators + Sheets helpers (token buckets, TTL caches)
from utils import (
    send_boot_notice_once, send_system_online_once, send_telegram_message_dedup,
    get_ws, get_records_cached, get_values_cached,
)
import gspread_guard

# ===== Utility helpers =====
def jitter(min_s=0.2, max_s=0.7):
    time.sleep(random.uniform(min_s, max_s))

def safe_call(label, fn, *args, sleep_after=0, **kwargs):
    try:
        print(f"▶ {label} …")
        out = fn(*args, **kwargs)
        if sleep_after:
            time.sleep(sleep_after)
        return out
    except Exception as e:
        print(f"❌ {label} error: {e}")

def threaded(fn, *args, **kwargs):
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return t

# ===== Modules (kept as you had them) =====
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
from claim_tracker import check_claims  # ← use the polished, batched version
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

# ===== Background loops =====
def start_flask_app():
    port = int(os.getenv("PORT", "10000"))
    print(f"🟢 Starting Flask app on port {port} …")
    telegram_app.run(host="0.0.0.0", port=port, debug=False)

def run_scheduler_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)

def start_watchdog_lazy():
    # Avoid import-time crashes if a stub is missing; start when ready.
    try:
        from nova_watchdog import run_watchdog
        print("🔍 Starting Watchdog…")
        run_watchdog()
    except Exception as e:
        print(f"⚠️ Watchdog start skipped: {e}")

def load_presale_stream():
    """Utils-based loader (uses cached client + backoff); no raw gspread here."""
    print("⚙️ Attempting to load worksheet: Presale_Stream")
    try:
        ws = get_ws("Presale_Stream")
        # Light-touch read to confirm; uses TTL cache under the hood
        _ = get_values_cached("Presale_Stream", ttl_s=120)
        print("✅ Loaded worksheet: Presale_Stream")
        return ws
    except Exception as e:
        print(f"❌ Failed to load Presale_Stream: {e}")
        return None

def start_staking_yield_loop():
    def loop():
        from staking_yield_tracker import run_staking_yield_tracker
        while True:
            print("🔁 Checking staking yield…")
            safe_call("staking_yield_tracker", run_staking_yield_tracker)
            time.sleep(21600)  # 6h
    threaded(loop)

# ===== Main boot =====
if __name__ == "__main__":
    print("📡 Orion Cloud Boot Sequence Initiated")

    # Arm Telegram webhook & bring up infra threads
    safe_call("Set Telegram webhook", set_telegram_webhook)
    threaded(start_flask_app)
    threaded(run_scheduler_loop)
    threaded(run_orion_voice_loop)

    # One-time boot notices (de-duped)
    send_boot_notice_once("🟢 NovaTrade system booted and live.")
    send_system_online_once()

    # Start watchdog after infra is up
    start_watchdog_lazy()

    # ---- Gentle boot: serialize the first heavy minute ----
    rotation_ws = load_presale_stream()
    if rotation_ws:
        jitter(); safe_call("ROI tracker boot pass", scan_roi_tracking, sleep_after=2)
        jitter(); safe_call("Milestone alerts", run_milestone_alerts, sleep_after=2)
        safe_call("Heartbeat: ROI Tracker", log_heartbeat, "ROI Tracker", "Boot pass complete")
    else:
        print("⛔ Presale_Stream unavailable — presale scan skipped")

    # Vault & summaries (short staggering to avoid 429s)
    jitter(); safe_call("Token vault sync", sync_token_vault, sleep_after=1)
    jitter(); safe_call("Top token summary", run_top_token_summary, sleep_after=1)
    jitter(); safe_call("Vault intelligence", run_vault_intelligence, sleep_after=1)

    print("🚀 Executing any pending vault rotations…")
    jitter(); safe_call("Vault rotation gate (MIND)", gate_vault_rotation, "MIND", sleep_after=1)
    jitter(); safe_call("Vault rotation executor", run_vault_rotation_executor, sleep_after=2)

    print("📋 Syncing Scout Decisions → Rotation_Planner…")
    jitter(); safe_call("Scout→Planner sync", sync_rotation_planner, sleep_after=1)

    print("📅 Syncing ROI feedback responses…")
    jitter(); safe_call("ROI feedback sync", run_roi_feedback_sync, sleep_after=1)

    print("📰 Sentiment Radar (boot pass)…")
    jitter(); safe_call("Sentiment radar", run_sentiment_radar, sleep_after=1)

    jitter(); 
    on, note = check_nova_trigger()
    if on:
        trigger_nova_ping("NOVA UPDATE", note or "Signal is ON.")   

    # 🔕 CLAIM LOGIC: delay and schedule (no boot storm)
    def _late_claims():
        time.sleep(90)  # let early readers finish
        safe_call("Claim tracker (delayed)", check_claims, sleep_after=1)
        # optional decision prompt pass shortly after
        time.sleep(15)
        safe_call("Claim decision prompt (delayed)", run_claim_decision_prompt, sleep_after=1)
    threaded(_late_claims)

    if rotation_ws:
        print("⏰ Running presale scan every 60 min")
        jitter(); safe_call("Presale scorer", run_presale_scorer, sleep_after=1)
    else:
        print("⛔ Presale_Stream unavailable — presale scan skipped")

    # ===== Schedules (kept; added gentle offsets where it matters) =====
    schedule.every(60).minutes.do(run_rotation_log_updater)
    schedule.every(60).minutes.do(run_rebalance_scanner)
    schedule.every(60).minutes.at(":33").do(run_rotation_memory)
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

    # NEW: claim checks on a gentle minute to avoid collisions
    schedule.every().hour.at(":07").do(check_claims)
    schedule.every().hour.at(":09").do(run_claim_decision_prompt)

    # ===== Continuous workers that aren’t purely scheduled =====
    threaded(run_stalled_asset_detector)
    time.sleep(2)
    start_staking_yield_loop()
    time.sleep(2)

    print("🪚 Cleaning Rotation_Log ROI column…")
    time.sleep(2)

    threaded(run_rotation_stats_sync)
    time.sleep(2)

    threaded(run_memory_weight_sync)
    time.sleep(2)

    print("🧠 Calculating Total Memory Score…")
    jitter(); safe_call("Total memory score sync", run_memory_score_sync, sleep_after=1)

    threaded(run_rebuy_roi_tracker)
    time.sleep(2)

    jitter(); safe_call("Rotation feedback engine", run_rotation_feedback_engine, sleep_after=1)

    print("📊 Running Performance Dashboard…")
    jitter(); safe_call("Performance dashboard", run_performance_dashboard, sleep_after=1)

    print("🔁 Initial rebalance scan…")
    jitter(); safe_call("Rebalance scanner", run_rebalance_scanner, sleep_after=1)

    print("📢 Telegram Summary Layer…")
    jitter(); safe_call("Telegram summary", run_telegram_summary, sleep_after=1)

    print("🧠 Rotation Memory Sync…")
    jitter(); safe_call("Rotation memory sync", run_rotation_memory, sleep_after=1)

    print("🔁 Undersized rebuy engine…")
    jitter(); safe_call("Undersized rebuy", run_undersized_rebuy, sleep_after=1)

    print("♻️ Memory-aware rebuy engine…")
    jitter(); safe_call("Memory rebuy scan", run_memory_rebuy_scan, sleep_after=1)

    print("🧠 Rebuy Weights…")
    jitter(); safe_call("Rebuy weight calc", run_rebuy_weight_calculator, sleep_after=1)

    print("🚨 Sentiment-Triggered Rebuy Scan…")
    jitter(); safe_call("Sentiment trigger engine", run_sentiment_trigger_engine, sleep_after=1)

    jitter(); safe_call("Memory scoring", run_memory_scoring, sleep_after=1)

    print("🧮 Suggested Target Calculator…")
    jitter(); safe_call("Portfolio weight adjuster", run_portfolio_weight_adjuster, sleep_after=1)

    print("📊 Sync Suggested % → Target %…")
    jitter(); safe_call("Target % updater", run_target_percent_updater, sleep_after=1)

    jitter(); safe_call("Total memory score sync (final)", sync_total_memory_score, sleep_after=1)

    print("📊 Syncing Vault Tags → Rotation_Stats…")
    threaded(run_vault_to_stats_sync)
    time.sleep(2)

    jitter(); safe_call("Vault alerts", run_vault_alerts, sleep_after=1)

    print("🔔 Vault Intelligence Alerts…")
    jitter(); safe_call("Vault alerts (2nd pass)", run_vault_alerts, sleep_after=1)

    print("📦 Syncing Vault ROI + Memory Stats…")
    jitter(); safe_call("Vault growth sync", run_vault_growth_sync, sleep_after=1)

    print("📈 Writing daily snapshot to Vault ROI Tracker…")
    jitter(); safe_call("Vault ROI tracker", run_vault_roi_tracker, sleep_after=1)

    print("📬 Vault Review Alerts…")
    jitter(); safe_call("Vault review alerts", run_vault_review_alerts, sleep_after=1)

    print("🔁 Scanning vaults for decay…")
    jitter(); safe_call("Vault rotation scanner", run_vault_rotation_scanner, sleep_after=1)

    # Binance executor: opt-in
    if os.getenv("ENABLE_CLOUD_BINANCE", "false").lower() == "true":
        try:
            from rotation_binance_executor import run_rotation_binance_executor
            safe_call("Binance executor", run_rotation_binance_executor)
        except Exception as e:
            print(f"⚠️ Skipping Binance executor: {e}")
    else:
        print("⚠️ Binance executor skipped (ENABLE_CLOUD_BINANCE is false)")
        print("📋 Auto-Confirm Planner…")
        safe_call("Auto-confirm planner", run_auto_confirm_planner)
        safe_call("Vault memory evaluate", evaluate_vault_memory)
        safe_call("Vault memory importer", run_vault_memory_importer)

        time.sleep(2)
        jitter(); safe_call("Unlock horizon alerts", run_unlock_horizon_alerts)

    print("💥 run_presale_scorer() BOOTED")
    send_boot_notice_once()
    print("🧠 NovaTrade system is live.")
