# main.py — NT3.0 boot polish (quiet + quota-safe + serialized cold boot)
import os, time, threading, schedule
from functools import partial

from flask import Flask
from telegram_webhook import telegram_app, set_telegram_webhook

# === Import jobs (all optional; missing ones will be soft-skipped) ============
def _try_import(path, name):
    try:
        mod = __import__(path, fromlist=[name])
        return getattr(mod, name)
    except Exception as e:
        # Soft-skip missing/renamed modules; log once
        from utils import warn
        warn(f"Import skipped: {path}.{name} ({e})")
        return None

run_watchdog                 = _try_import("nova_watchdog", "run_watchdog")
scan_rotation_candidates     = _try_import("rotation_signal_engine", "scan_rotation_candidates")
scan_roi_tracking            = _try_import("roi_tracker", "scan_roi_tracking")
run_milestone_alerts         = _try_import("milestone_alerts", "run_milestone_alerts")
sync_token_vault             = _try_import("token_vault_sync", "sync_token_vault")
sync_rotation_planner        = _try_import("scout_to_planner_sync", "sync_rotation_planner")
run_presale_scorer           = _try_import("presale_scorer", "run_presale_scorer")
check_nova_trigger           = _try_import("nova_trigger_watcher", "check_nova_trigger")
run_roi_feedback_sync        = _try_import("roi_feedback_sync", "run_roi_feedback_sync")
trigger_nova_ping            = _try_import("nova_trigger", "trigger_nova_ping")
run_orion_voice_loop         = _try_import("orion_voice_loop", "run_orion_voice_loop")
log_heartbeat                = _try_import("nova_heartbeat", "log_heartbeat")
run_stalled_asset_detector   = _try_import("stalled_asset_detector", "run_stalled_asset_detector")
check_claims                 = _try_import("claim_tracker", "check_claims")
run_sentiment_radar          = _try_import("sentiment_radar", "run_sentiment_radar")
run_sentiment_summary        = _try_import("sentiment_summary", "run_sentiment_summary")
run_rotation_stats_sync      = _try_import("rotation_stats_sync", "run_rotation_stats_sync")
run_rotation_feedback_engine = _try_import("rotation_feedback_engine", "run_rotation_feedback_engine")
run_performance_dashboard    = _try_import("performance_dashboard", "run_performance_dashboard")
run_rebalance_scanner        = _try_import("rebalance_scanner", "run_rebalance_scanner")
run_telegram_summaries       = _try_import("telegram_summaries", "run_telegram_summaries")
run_rotation_memory          = _try_import("rotation_memory", "run_rotation_memory")
run_rotation_log_updater     = _try_import("rotation_log_updater", "run_rotation_log_updater")
run_portfolio_weight_adjuster= _try_import("portfolio_weight_adjuster", "run_portfolio_weight_adjuster")
run_target_percent_updater   = _try_import("target_percent_updater", "run_target_percent_updater")
run_undersized_rebuy         = _try_import("rebuy_engine", "run_undersized_rebuy")
run_memory_rebuy_scan        = _try_import("rebuy_memory_engine", "run_memory_rebuy_scan")
run_rebuy_roi_tracker        = _try_import("rebuy_roi_tracker", "run_rebuy_roi_tracker")
run_memory_scoring           = _try_import("rotation_memory_scoring", "run_memory_scoring")
run_vault_intelligence       = _try_import("vault_intelligence", "run_vault_intelligence")
run_vault_to_stats_sync      = _try_import("vault_to_stats_sync", "run_vault_to_stats_sync")
run_vault_alerts             = _try_import("vault_alerts_phase15d", "run_vault_alerts")
run_vault_growth_sync        = _try_import("vault_growth_sync", "run_vault_growth_sync")
run_vault_roi_tracker        = _try_import("vault_roi_tracker", "run_vault_roi_tracker")
run_vault_review_alerts      = _try_import("vault_review_alerts", "run_vault_review_alerts")
run_vault_rotation_scanner   = _try_import("vault_rotation_scanner", "run_vault_rotation_scanner")
run_vault_rotation_executor  = _try_import("vault_rotation_executor", "run_vault_rotation_executor")
run_wallet_monitor           = _try_import("wallet_monitor", "run_wallet_monitor")
run_unlock_horizon_alerts    = _try_import("unlock_horizon_alerts", "run_unlock_horizon_alerts")
run_top_token_summary        = _try_import("top_token_summary", "run_top_token_summary")
run_auto_confirm_planner     = _try_import("auto_confirm_planner", "run_auto_confirm_planner")
run_memory_weight_sync       = _try_import("memory_weight_sync", "run_memory_weight_sync")
run_sentiment_trigger_engine = _try_import("sentiment_trigger_engine", "run_sentiment_trigger_engine")
run_roi_threshold_validator  = _try_import("roi_threshold_validator", "run_roi_threshold_validator")
run_sentiment_alerts         = _try_import("sentiment_alerts", "run_sentiment_alerts")
run_rebuy_weight_calculator  = _try_import("rebuy_weight_calculator", "run_rebuy_weight_calculator")
run_memory_score_sync        = _try_import("memory_score_sync", "run_memory_score_sync")

# === Utils ===================================================================
from utils import (
    get_gspread_client, send_telegram_message_dedup,
    send_boot_notice_once, send_system_online_once,
    info, warn, error, get_ws_cached, get_all_records_cached,
    with_sheets_gate, is_cold_boot
)

BOT_TOKEN  = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
SHEET_URL  = os.getenv("SHEET_URL")

# ===== infra helpers =========================================================
def start_flask_app():
    info("Starting Flask app on 0.0.0.0:10000…")
    telegram_app.run(host="0.0.0.0", port=10000)

def run_scheduler_loop():
    while True:
        schedule.run_pending()
        time.sleep(1)

def threaded(fn, *a, **k):
    t = threading.Thread(target=fn, args=a, kwargs=k, daemon=True); t.start(); return t

def _safe(label, fn, *a, **k):
    if fn is None:
        warn(f"Skip job: {label} not available")
        return
    try:
        info(f"▶ {label}")
        return fn(*a, **k)
    except Exception as e:
        error(f"{label} failed: {e}")

def _delay(min_s=0.6, max_s=0.9):
    time.sleep(min_s)

def _serial_boot(steps):
    """
    Run a list of (label, fn) steps serially with a small delay and a read gate.
    This prevents cold-boot 429s by spacing reads.
    """
    for label, fn in steps:
        with with_sheets_gate("read", tokens=1):
            _safe(label, fn)
        _delay()

def _bg(label, fn):
    return threaded(_safe, label, fn)

# ===== custom tiny loops =====================================================
def start_staking_yield_loop():
    def loop():
        f = _try_import("staking_yield_tracker", "run_staking_yield_tracker")
        while True:
            info("Checking staking yield…")
            try:
                if f: f()
            except Exception as e:
                warn(f"staking_yield_tracker error: {e}")
            time.sleep(21600)  # 6h
    threaded(loop)

# ===== main ==================================================================
if __name__ == "__main__":
    # 1) Webhook + boot ping
    set_telegram_webhook()
    send_boot_notice_once("🟢 NovaTrade system booted and live.")

    # 2) Core services
    threaded(start_flask_app); time.sleep(0.8)
    _bg("Orion voice loop", run_orion_voice_loop)

    # 3) Cold-boot serialized passes (NO concurrency here)
    cold_steps = [
        ("Watchdog",                         run_watchdog),
        ("ROI tracker (boot pass)",          scan_roi_tracking),
        ("Milestone alerts (boot pass)",     run_milestone_alerts),
        ("Token Vault sync",                 sync_token_vault),
        ("Top token summary",                run_top_token_summary),
        ("Vault intelligence",               run_vault_intelligence),
        ("Vault rotation executor",          run_vault_rotation_executor),
        ("Scout→Planner sync",               sync_rotation_planner),
        ("ROI feedback sync",                run_roi_feedback_sync),
        ("Sentiment Radar (boot pass)",      run_sentiment_radar),
        ("Check Nova trigger",               check_nova_trigger),
    ]
    _serial_boot(cold_steps)

    try:
        if trigger_nova_ping:
            trigger_nova_ping("NOVA UPDATE")
    except Exception as e:
        warn(f"Nova ping skipped: {e}")

    send_system_online_once()

    # 4) Background schedules (staggered)
    schedule.every(60).minutes.do(lambda: _safe("Rotation Log Updater", run_rotation_log_updater))
    schedule.every(60).minutes.do(lambda: _safe("Rebalance scanner", run_rebalance_scanner))
    schedule.every(60).minutes.do(lambda: _safe("Rotation memory sync", run_rotation_memory))
    schedule.every(6).hours.do(lambda: _safe("Sentiment Radar", run_sentiment_radar))
    schedule.every(3).hours.do(lambda: _safe("Memory-aware rebuy", run_memory_rebuy_scan))
    schedule.every(3).hours.do(lambda: _safe("Sentiment summary", run_sentiment_summary))

    schedule.every().day.at("01:00").do(lambda: _safe("ROI threshold validator", run_roi_threshold_validator))
    schedule.every().day.at("01:10").do(lambda: _safe("Rebuy weight calculator", run_rebuy_weight_calculator))
    schedule.every().day.at("01:15").do(lambda: _safe("Memory score sync", run_memory_score_sync))
    schedule.every().day.at("01:30").do(lambda: _safe("Top token summary", run_top_token_summary))
    schedule.every().day.at("02:00").do(lambda: _safe("Vault ROI tracker", run_vault_roi_tracker))
    schedule.every().day.at("09:15").do(lambda: _safe("Vault rotation scanner", run_vault_rotation_scanner))
    schedule.every().day.at("09:25").do(lambda: _safe("Vault rotation executor", run_vault_rotation_executor))
    schedule.every().day.at("09:45").do(lambda: _safe("Wallet monitor", run_wallet_monitor))
    schedule.every().day.at("12:45").do(lambda: _safe("Rebuy ROI tracker", run_rebuy_roi_tracker))
    schedule.every().day.at("13:00").do(lambda: _safe("Sentiment alerts", run_sentiment_alerts))

    _bg("Scheduler loop", run_scheduler_loop)
    _bg("Stalled asset detector (boot pass)", run_stalled_asset_detector)
    _bg("Claim tracker (boot pass)", check_claims)
    start_staking_yield_loop()

    # 5) Stats & memory syncs (serial to avoid spikes)
    serial2 = [
        ("Performance Dashboard",            run_performance_dashboard),
        ("Rotation Stats Sync",              run_rotation_stats_sync),
        ("Memory Weight Sync",               run_memory_weight_sync),
        ("Rebuy ROI Tracker",                run_rebuy_roi_tracker),
        ("Rotation feedback engine",         run_rotation_feedback_engine),
        ("Rotation memory sync",             run_rotation_memory),
        ("Undersized rebuy engine",          run_undersized_rebuy),
        ("Memory-aware rebuy engine",        run_memory_rebuy_scan),
        ("Sentiment-Triggered Rebuy Scan",   run_sentiment_trigger_engine),
        ("Memory scoring",                   run_memory_scoring),
        ("Suggested Target Calculator",      run_portfolio_weight_adjuster),
        ("Sync Suggested % → Target %",      run_target_percent_updater),
        ("Vault Tags → Rotation_Stats",      run_vault_to_stats_sync),
        ("Vault alerts",                     run_vault_alerts),
        ("Vault growth sync",                run_vault_growth_sync),
        ("Vault ROI tracker",                run_vault_roi_tracker),
        ("Vault review alerts",              run_vault_review_alerts),
        ("Vault rotation scanner",           run_vault_rotation_scanner),
        ("Auto-Confirm Planner",             run_auto_confirm_planner),
        ("Unlock horizon alerts",            run_unlock_horizon_alerts),
        ("Telegram Summaries",               run_telegram_summaries),
    ]
    _serial_boot(serial2)

    send_telegram_message_dedup("✅ NovaTrade boot sequence complete.", key="boot_done")
