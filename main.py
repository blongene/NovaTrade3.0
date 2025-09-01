# main.py — NovaTrade 3.0 resilient boot (Render-safe, quota-safe, lazy imports)
import os, time, threading, random, importlib
import schedule

from utils import (
    info, warn, error,
    send_boot_notice_once, send_system_online_once, send_telegram_message_dedup,
    is_cold_boot,
)

# ---------------------------------------------------------------------------
# Flask / Telegram webhook handling (robust to missing telegram_webhook module)
# ---------------------------------------------------------------------------
def _load_telegram_webhook():
    """
    Tries to import telegram_webhook.{telegram_app,set_telegram_webhook}.
    Falls back to a minimal Flask app with /healthz if missing.
    """
    try:
        mod = importlib.import_module("telegram_webhook")
        telegram_app = getattr(mod, "telegram_app", None)
        set_telegram_webhook = getattr(mod, "set_telegram_webhook", None)
        if telegram_app is None or set_telegram_webhook is None:
            raise AttributeError("telegram_app or set_telegram_webhook missing")
        return telegram_app, set_telegram_webhook
    except Exception as e:
        warn(f"telegram_webhook not available ({e}); using minimal Flask app")
        try:
            from flask import Flask
            app = Flask(__name__)

            @app.get("/healthz")
            def _health():
                return {"ok": True, "service": "NovaTrade minimal webhook"}, 200

            def _no_webhook():
                info("Webhook setup skipped (minimal Flask app mode).")
            return app, _no_webhook
        except Exception as ee:
            # Absolute last resort: null app that doesn't crash boot
            warn(f"Flask unavailable ({ee}); webhook disabled")
            return None, (lambda: None)

telegram_app, set_telegram_webhook = _load_telegram_webhook()

def _start_flask_app():
    if telegram_app is None:
        warn("No Flask app available; skipping HTTP server.")
        return
    port = int(os.getenv("PORT", "10000"))
    host = os.getenv("HOST", "0.0.0.0")
    info(f"Starting Flask app on {host}:{port}…")
    # Avoid debug reloader in Render
    telegram_app.run(host=host, port=port, debug=False, use_reloader=False)

# ---------------------------------------------------------------------------
# Lazy import helpers
# ---------------------------------------------------------------------------
def _safe_import(module_name, fn_name=None, required=False):
    """
    Import a module (and optional function). If missing, returns (None or no-op).
    When required=True and import fails, we still return a no-op so boot proceeds.
    """
    try:
        mod = importlib.import_module(module_name)
        if fn_name:
            fn = getattr(mod, fn_name, None)
            if fn is None:
                raise AttributeError(f"{fn_name} missing in {module_name}")
            return fn
        return mod
    except Exception as e:
        msg = f"Import skipped: {module_name}{'.'+fn_name if fn_name else ''} ({e})"
        if required:
            warn(msg + " -> using no-op")
            return lambda *a, **k: None
        else:
            warn(msg)
            return None

def threaded(fn, *args, **kwargs):
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return t

def _safe(label, fn, *args, **kwargs):
    """
    Run a callable with logging; never throw.
    """
    try:
        info(f"▶ {label}")
        return fn(*args, **kwargs)
    except Exception as e:
        error(f"{label} failed: {e}")

def _staggered_sleep(min_s=0.45, max_s=1.20):
    time.sleep(random.uniform(min_s, max_s))

# ---------------------------------------------------------------------------
# Schedulers
# ---------------------------------------------------------------------------
def _run_scheduler_loop():
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            warn(f"schedule.run_pending error: {e}")
        time.sleep(1)

# ---------------------------------------------------------------------------
# Optional background loop: staking yield tracker (kept from prior design)
# ---------------------------------------------------------------------------
def _start_staking_yield_loop():
    run_staking_yield_tracker = _safe_import("staking_yield_tracker", "run_staking_yield_tracker")
    if run_staking_yield_tracker is None:
        return
    def loop():
        while True:
            try:
                info("Checking staking yield…")
                run_staking_yield_tracker()
            except Exception as e:
                warn(f"staking_yield_tracker error: {e}")
            time.sleep(6 * 60 * 60)  # every 6h
    threaded(loop)

# ---------------------------------------------------------------------------
# Job registry (labels + module/function names)
#   - All imports are lazy and optional (no boot crashes).
#   - If fn is None/not found, job is silently skipped with a WARN.
# ---------------------------------------------------------------------------
BOOT_PASS = [
    ("Watchdog",                       ("nova_watchdog",            "run_watchdog")),
    ("ROI tracker (boot pass)",        ("roi_tracker",              "scan_roi_tracking")),
    ("Milestone alerts (boot pass)",   ("milestone_alerts",         "run_milestone_alerts")),
    ("Token Vault sync",               ("token_vault_sync",         "sync_token_vault")),
    ("Scout→Planner sync",             ("scout_to_planner_sync",    "sync_rotation_planner")),
    ("Vault intelligence",             ("vault_intelligence",       "run_vault_intelligence")),
    ("Vault rotation executor",        ("vault_rotation_executor",  "run_vault_rotation_executor")),
    ("ROI feedback sync",              ("roi_feedback_sync",        "run_roi_feedback_sync")),
    ("Sentiment Radar (boot pass)",    ("sentiment_radar",          "run_sentiment_radar")),
    ("Check Nova trigger",             ("nova_trigger_watcher",     "check_nova_trigger")),
    ("Nova ping",                      ("nova_trigger",             "trigger_nova_ping")),
    ("Performance Dashboard",          ("performance_dashboard",    "run_performance_dashboard")),
    ("Stalled asset detector",         ("stalled_asset_detector",   "run_stalled_asset_detector")),
    ("Claim tracker",                  ("claim_tracker",            "check_claims")),
    ("Top token summary",              ("top_token_summary",        "run_top_token_summary")),
    ("Vault alerts",                   ("vault_alerts_phase15d",    "run_vault_alerts")),
    ("Vault growth sync",              ("vault_growth_sync",        "run_vault_growth_sync")),
    ("Vault ROI tracker",              ("vault_roi_tracker",        "run_vault_roi_tracker")),
    ("Vault review alerts",            ("vault_review_alerts",      "run_vault_review_alerts")),
    ("Vault rotation scanner",         ("vault_rotation_scanner",   "run_vault_rotation_scanner")),
    ("Auto-Confirm Planner",           ("auto_confirm_planner",     "run_auto_confirm_planner")),
    ("Unlock horizon alerts",          ("unlock_horizon_alerts",    "run_unlock_horizon_alerts")),
    ("Rotation feedback engine",       ("rotation_feedback_engine", "run_rotation_feedback_engine")),
]

# Recurring jobs (schedule)
SCHEDULED = [
    # frequent cadence
    (("rotation_log_updater",  "run_rotation_log_updater"), "every(60).minutes"),
    (("rebalance_scanner",     "run_rebalance_scanner"),    "every(60).minutes"),
    (("rotation_memory",       "run_rotation_memory"),      "every(60).minutes"),
    (("sentiment_radar",       "run_sentiment_radar"),      "every(6).hours"),
    (("rebuy_memory_engine",   "run_memory_rebuy_scan"),    "every(3).hours"),
    (("sentiment_summary",     "run_sentiment_summary"),    "every(3).hours"),

    # daily cadence
    (("roi_threshold_validator","run_roi_threshold_validator"), "every().day.at('01:00')"),
    (("rebuy_weight_calculator","run_rebuy_weight_calculator"), "every().day.at('01:10')"),
    (("memory_score_sync",      "run_memory_score_sync"),       "every().day.at('01:15')"),
    (("top_token_summary",      "run_top_token_summary"),       "every().day.at('01:30')"),
    (("vault_roi_tracker",      "run_vault_roi_tracker"),       "every().day.at('02:00')"),
    (("vault_rotation_scanner", "run_vault_rotation_scanner"),  "every().day.at('09:15')"),
    (("vault_rotation_executor","run_vault_rotation_executor"), "every().day.at('09:25')"),
    (("wallet_monitor",         "run_wallet_monitor"),          "every().day.at('09:45')"),
    (("rebuy_roi_tracker",      "run_rebuy_roi_tracker"),       "every().day.at('12:45')"),
    (("sentiment_alerts",       "run_sentiment_alerts"),        "every().day.at('13:00')"),
]

# Fire-and-forget (threaded) helpers
THREAD_START = [
    ("Rotation Stats Sync (bg)",      ("rotation_stats_sync",   "run_rotation_stats_sync")),
    ("Memory Weight Sync (bg)",       ("memory_weight_sync",    "run_memory_weight_sync")),
    ("Rebuy ROI Tracker (bg)",        ("rebuy_roi_tracker",     "run_rebuy_roi_tracker")),
    ("Telegram Summaries (bg)",       ("telegram_summaries",    "run_telegram_summaries")),
]

# One-shot sequenced tasks after boot (with soft delays)
POST_BOOT = [
    ("Rotation memory sync",           ("rotation_memory",       "run_rotation_memory")),
    ("Undersized rebuy engine",        ("rebuy_engine",          "run_undersized_rebuy")),
    ("Memory-aware rebuy engine",      ("rebuy_memory_engine",   "run_memory_rebuy_scan")),
    ("Rebuy weight calculator",        ("rebuy_weight_calculator","run_rebuy_weight_calculator")),
    ("Sentiment trigger engine",       ("sentiment_trigger_engine","run_sentiment_trigger_engine")),
    ("Memory scoring",                 ("rotation_memory_scoring","run_memory_scoring")),
    ("Portfolio weight adjuster",      ("portfolio_weight_adjuster","run_portfolio_weight_adjuster")),
    ("Target % updater",               ("target_percent_updater","run_target_percent_updater")),
    ("Vault→Stats sync (bg)",          ("vault_to_stats_sync",   "run_vault_to_stats_sync")),
]

def _call_module_fn(module_name, fn_name, *args, **kwargs):
    fn = _safe_import(module_name, fn_name)
    if fn is None:
        warn(f"Skip job: {module_name}.{fn_name} not available")
        return
    return fn(*args, **kwargs)

def _schedule_jobs():
    for (mod_fn, cron_expr) in SCHEDULED:
        mod, fn = mod_fn
        try:
            # Resolve function once; capture in closure for schedule call
            target = _safe_import(mod, fn)
            if target is None:
                warn(f"Schedule skip: {mod}.{fn} not available")
                continue
            # Build schedule call from string, e.g. "every(60).minutes"
            # or "every().day.at('01:00')"
            sch = schedule
            # Evaluate the cron expression safely
            # We only allow attribute chain: every(...).minutes / every().day.at('..')
            # Simple parser:
            if cron_expr.startswith("every("):
                # Examples:
                #   every(60).minutes
                #   every().day.at('01:00')
                expr = cron_expr
                # Construct schedule object
                # This is a tiny interpreter for the two forms we use:
                if ".minutes" in expr:
                    n = int(expr.split("every(")[1].split(")")[0])
                    sch.every(n).minutes.do(target)
                elif ".hours" in expr:
                    n = int(expr.split("every(")[1].split(")")[0])
                    sch.every(n).hours.do(target)
                elif ".day.at(" in expr:
                    hhmm = expr.split(".day.at(")[1].split(")")[0].strip("'\"")
                    sch.every().day.at(hhmm).do(target)
                else:
                    # default fallback
                    sch.every(60).minutes.do(target)
            else:
                # unknown schedule; default to hourly
                sch.every(60).minutes.do(target)
            info(f"Scheduled: {mod}.{fn} [{cron_expr}]")
            _staggered_sleep(0.05, 0.12)
        except Exception as e:
            warn(f"Failed to schedule {mod}.{fn}: {e}")

if __name__ == "__main__":
    # 1) Webhook + boot notice
    try:
        set_telegram_webhook()
        send_boot_notice_once("🟢 NovaTrade system booted and live.")
    except Exception as e:
        warn(f"Webhook/boot notice issue: {e}")

    # 2) Core services: HTTP + scheduler + staking loop
    threaded(_start_flask_app)
    _staggered_sleep(0.2, 0.5)
    threaded(_run_scheduler_loop)
    _staggered_sleep(0.2, 0.4)
    _start_staking_yield_loop()

    # 3) Initial boot pass (serialized w/ jitter to reduce 429s)
    #    We keep order similar to your prior boot plan.
    cold = is_cold_boot()
    info(f"Cold boot window: {cold}")
    for label, (mod, fn) in BOOT_PASS:
        _safe(label, _call_module_fn, mod, fn)
        _staggered_sleep(0.55, 1.10)

    # 4) Background threads that can run continuously
    for label, (mod, fn) in THREAD_START:
        target = _safe_import(mod, fn)
        if target:
            info(f"Starting background: {label}")
            threaded(target)
            _staggered_sleep(0.15, 0.35)

    # 5) Recurring schedules
    _schedule_jobs()

    # 6) Post-boot sequence (spaced to avoid burst)
    for label, (mod, fn) in POST_BOOT:
        _safe(label, _call_module_fn, mod, fn)
        _staggered_sleep(0.55, 1.05)

    # 7) Final pings
    try:
        send_system_online_once()
        send_telegram_message_dedup("✅ NovaTrade boot sequence complete.", key="boot_done")
    except Exception as e:
        warn(f"Final boot ping skipped: {e}")

    # 8) Idle forever (the scheduler & Flask threads do the work)
    info("NovaTrade main thread idle. Scheduler and HTTP server are running.")
    while True:
        time.sleep(60)
