# main.py — NovaTrade 3.0 boot polish (quiet, quota-safe, lazy imports)
import os, time, threading, traceback, importlib, schedule
from typing import Callable, Optional

# Utils (logging + boot pings)
from utils import (
    info, warn, error,
    send_boot_notice_once, send_system_online_once, send_telegram_message_dedup,
    is_cold_boot,
)

# ---------------------------
# Flask / Telegram webhook
# ---------------------------
def _load_telegram_webhook():
    try:
        m = importlib.import_module("telegram_webhook")
        telegram_app = getattr(m, "telegram_app", None)
        set_telegram_webhook = getattr(m, "set_telegram_webhook", None)
        if telegram_app is None or set_telegram_webhook is None:
            warn("telegram_webhook module present but missing attributes.")
            return None, None
        return telegram_app, set_telegram_webhook
    except Exception as e:
        warn(f"telegram_webhook not available: {e}")
        return None, None

def start_flask_app():
    app, _ = _load_telegram_webhook()
    if app is None:
        warn("Flask app not started (telegram_webhook unavailable).")
        return
    port = int(os.getenv("PORT", "10000"))
    info(f"Starting Flask app on port {port}…")
    try:
        app.run(host="0.0.0.0", port=port)
    except Exception as e:
        warn(f"Flask app exited: {e}")

def set_webhook_if_available():
    _, set_hook = _load_telegram_webhook()
    if set_hook is None:
        return
    try:
        set_hook()
        info("Set Telegram webhook ✅")
    except Exception as e:
        warn(f"Set Telegram webhook failed: {e}")

# ---------------------------
# Helpers
# ---------------------------
DISABLED = {s.strip() for s in os.getenv("NT_DISABLE", "").split(",") if s.strip()}
DISABLE_ALL_SCHEDULES = os.getenv("NT_DISABLE_ALL_SCHEDULES", "0") == "1"

def _disabled(name: str) -> bool:
    # allow disabling by exact "module.func" or by human label
    return name in DISABLED

def threaded(fn: Callable, *args, **kwargs):
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return t

def _resolve_func(path: str) -> Optional[Callable]:
    """
    path = 'module:function' or 'module.function'
    """
    p = path.replace(":", ".")
    if "." not in p:
        warn(f"Bad func path (no dot): {path}")
        return None
    mod, func = p.rsplit(".", 1)
    try:
        m = importlib.import_module(mod)
    except Exception as e:
        warn(f"Import failed for {mod}: {e}")
        return None
    f = getattr(m, func, None)
    if not callable(f):
        warn(f"{path} not callable or missing.")
        return None
    return f

def _safe(label: str, func: Callable, *args, **kwargs):
    if _disabled(label):
        info(f"⏭️ Skipping disabled task: {label}")
        return
    try:
        info(f"▶ {label}")
        return func(*args, **kwargs)
    except Exception as e:
        tb = traceback.format_exc(limit=2)
        error(f"{label} failed: {e}\n{tb}")

def _safe_path(label: str, func_path: str, *args, **kwargs):
    if _disabled(label) or _disabled(func_path):
        info(f"⏭️ Skipping disabled task: {label or func_path}")
        return
    func = _resolve_func(func_path)
    if func is None:
        warn(f"{label} skipped (unavailable): {func_path}")
        return
    return _safe(label, func, *args, **kwargs)

def run_scheduler_loop():
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            warn(f"schedule.run_pending error: {e}")
        time.sleep(1)

def schedule_safe(label: str, func_path: str, reg_callable: Callable):
    """
    reg_callable is something like: lambda f: schedule.every().day.at("01:00").do(f)
    """
    if DISABLE_ALL_SCHEDULES:
        info(f"⏸️ All schedules disabled; not registering: {label}")
        return
    if _disabled(label) or _disabled(func_path):
        info(f"⏭️ Not scheduling disabled task: {label}")
        return
    func = _resolve_func(func_path)
    if func is None:
        warn(f"Not scheduling {label}; function missing: {func_path}")
        return
    try:
        reg_callable(lambda: _safe(label, func))
        info(f"⏱️ Scheduled: {label}")
    except Exception as e:
        warn(f"Failed to schedule {label}: {e}")

# ---------------------------
# Optional background loops
# ---------------------------
def start_staking_yield_loop():
    """Runs staking_yield_tracker every 6h with internal guard."""
    def loop():
        f = _resolve_func("staking_yield_tracker.run_staking_yield_tracker")
        if f is None:
            warn("staking_yield_tracker unavailable; loop not started.")
            return
        while True:
            _safe("Staking Yield Tracker", f)
            time.sleep(6 * 60 * 60)  # 6h
    threaded(loop)

# ---------------------------
# Boot plan
# ---------------------------
BOOT_STAGGER_S = float(os.getenv("BOOT_STAGGER_SEC", "0.7"))

# Ordered boot tasks to minimize 429s during the first minute
BOOT_TASKS = [
    ("Watchdog",                         "nova_watchdog.run_watchdog"),
    ("ROI tracker (boot pass)",          "roi_tracker.scan_roi_tracking"),
    ("Milestone alerts (boot pass)",     "milestone_alerts.run_milestone_alerts"),
    ("Vault sync",                       "token_vault_sync.sync_token_vault"),
    ("Top token summary",                "top_token_summary.run_top_token_summary"),
    ("Vault intelligence",               "vault_intelligence.run_vault_intelligence"),
    ("Vault rotation executor",          "vault_rotation_executor.run_vault_rotation_executor"),
    ("Scout→Planner sync",               "scout_to_planner_sync.sync_rotation_planner"),
    ("ROI feedback sync",                "roi_feedback_sync.run_roi_feedback_sync"),
    ("Sentiment Radar (boot pass)",      "sentiment_radar.run_sentiment_radar"),
    ("Check Nova trigger",               "nova_trigger_watcher.check_nova_trigger"),
    ("Nova ping",                        "nova_trigger.trigger_nova_ping"),
]

# One-shot tasks after schedules are running
POST_SCHEDULE_TASKS = [
    ("Stalled asset detector",           "stalled_asset_detector.run_stalled_asset_detector"),
    ("Claim tracker",                    "claim_tracker.check_claims"),
    ("Performance Dashboard",            "performance_dashboard.run_performance_dashboard"),
    ("Rebalance scan (initial)",         "rebalance_scanner.run_rebalance_scanner"),
    ("Telegram summaries",               "telegram_summaries.run_telegram_summaries"),
    ("Rotation memory sync",             "rotation_memory.run_rotation_memory"),
    ("Undersized rebuy engine",          "rebuy_engine.run_undersized_rebuy"),
    ("Memory-aware rebuy engine",        "rebuy_memory_engine.run_memory_rebuy_scan"),
    ("Rebuy weight calculator",          "rebuy_weight_calculator.run_rebuy_weight_calculator"),
    ("Sentiment trigger engine",         "sentiment_trigger_engine.run_sentiment_trigger_engine"),
    ("Memory scoring",                   "rotation_memory_scoring.run_memory_scoring"),
    ("Portfolio weight adjuster",        "portfolio_weight_adjuster.run_portfolio_weight_adjuster"),
    ("Target percent updater",           "target_percent_updater.run_target_percent_updater"),
    ("Vault→Stats sync",                 "vault_to_stats_sync.run_vault_to_stats_sync"),
    ("Vault alerts",                     "vault_alerts_phase15d.run_vault_alerts"),
    ("Vault growth sync",                "vault_growth_sync.run_vault_growth_sync"),
    ("Vault ROI tracker",                "vault_roi_tracker.run_vault_roi_tracker"),
    ("Vault review alerts",              "vault_review_alerts.run_vault_review_alerts"),
    ("Vault rotation scanner",           "vault_rotation_scanner.run_vault_rotation_scanner"),
    ("Auto-confirm planner",             "auto_confirm_planner.run_auto_confirm_planner"),
    ("Unlock horizon alerts",            "unlock_horizon_alerts.run_unlock_horizon_alerts"),
]

# ---------------------------
# Schedules (guarded)
# ---------------------------
def register_schedules():
    # frequent
    schedule_safe("Rotation Log Updater (60m)", "rotation_log_updater.run_rotation_log_updater",
                  lambda f: schedule.every(60).minutes.do(f))
    schedule_safe("Rebalance Scanner (60m)",    "rebalance_scanner.run_rebalance_scanner",
                  lambda f: schedule.every(60).minutes.do(f))
    schedule_safe("Rotation Memory (60m)",      "rotation_memory.run_rotation_memory",
                  lambda f: schedule.every(60).minutes.do(f))
    schedule_safe("Sentiment Radar (6h)",       "sentiment_radar.run_sentiment_radar",
                  lambda f: schedule.every(6).hours.do(f))
    schedule_safe("Memory Rebuy Scan (3h)",     "rebuy_memory_engine.run_memory_rebuy_scan",
                  lambda f: schedule.every(3).hours.do(f))
    schedule_safe("Sentiment Summary (3h)",     "sentiment_summary.run_sentiment_summary",
                  lambda f: schedule.every(3).hours.do(f))

    # daily cadence (spread by minutes)
    schedule_safe("ROI Threshold Validator 01:00",   "roi_threshold_validator.run_roi_threshold_validator",
                  lambda f: schedule.every().day.at("01:00").do(f))
    schedule_safe("Rebuy Weight Calculator 01:10",   "rebuy_weight_calculator.run_rebuy_weight_calculator",
                  lambda f: schedule.every().day.at("01:10").do(f))
    schedule_safe("Memory Score Sync 01:15",         "memory_score_sync.run_memory_score_sync",
                  lambda f: schedule.every().day.at("01:15").do(f))
    schedule_safe("Top Token Summary 01:30",         "top_token_summary.run_top_token_summary",
                  lambda f: schedule.every().day.at("01:30").do(f))
    schedule_safe("Vault ROI Tracker 02:00",         "vault_roi_tracker.run_vault_roi_tracker",
                  lambda f: schedule.every().day.at("02:00").do(f))
    schedule_safe("Vault Rotation Scanner 09:15",    "vault_rotation_scanner.run_vault_rotation_scanner",
                  lambda f: schedule.every().day.at("09:15").do(f))
    schedule_safe("Vault Rotation Executor 09:25",   "vault_rotation_executor.run_vault_rotation_executor",
                  lambda f: schedule.every().day.at("09:25").do(f))
    schedule_safe("Wallet Monitor 09:45",            "wallet_monitor.run_wallet_monitor",
                  lambda f: schedule.every().day.at("09:45").do(f))
    schedule_safe("Rebuy ROI Tracker 12:45",         "rebuy_roi_tracker.run_rebuy_roi_tracker",
                  lambda f: schedule.every().day.at("12:45").do(f))
    schedule_safe("Sentiment Alerts 13:00",          "sentiment_alerts.run_sentiment_alerts",
                  lambda f: schedule.every().day.at("13:00").do(f))

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    # Webhook + boot notices (safe if telegram disabled)
    set_webhook_if_available()
    send_boot_notice_once("🟢 NovaTrade system booted and live.")

    # Core servers
    threaded(start_flask_app)
    time.sleep(1.0)

    # Early background loop (if available)
    start_staking_yield_loop()
    time.sleep(0.3)

    # Boot tasks (serialized & staggered to reduce RPMS)
    for label, func_path in BOOT_TASKS:
        if "trigger_nova_ping" in func_path:
            # historical call expects a string arg; only if callable exists
            f = _resolve_func(func_path)
            if f:
                _safe(label, f, "NOVA UPDATE")
        else:
            _safe_path(label, func_path)
        time.sleep(BOOT_STAGGER_S)

    send_system_online_once()

    # Register schedules then start the scheduler loop
    register_schedules()
    threaded(run_scheduler_loop)

    # Post-schedule tasks (staggered)
    for label, func_path in POST_SCHEDULE_TASKS:
        _safe_path(label, func_path)
        time.sleep(BOOT_STAGGER_S)

    # Final boot ping (de-duped already)
    send_telegram_message_dedup("✅ NovaTrade boot sequence complete.", key="boot_done")
