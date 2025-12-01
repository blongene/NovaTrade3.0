import time

# main.py — NovaTrade 3.0 (bullet-proof boot, lazy imports, quota-safe) — PHASE 9 PATCHED
import os, time, random, threading, schedule
from typing import Optional, Callable
import gspread_guard  # patches Worksheet methods (cache+gates+backoff)
import hmac, hashlib, json
from flask import Blueprint, request, jsonify
from policy_bias_engine import run_policy_bias_builder
from telegram_summaries import run_telegram_summaries
from stalled_autotrader import run_stalled_autotrader_shadow
from telemetry_mirror import _compact_wallet_monitor_if_needed

# Enable asynchronous Sheets gateway flusher
try:
    from sheets_bp import start_background_flusher
    start_background_flusher()
    print("[SheetsGateway] background flusher started", flush=True)
except Exception as e:
    print(f"[SheetsGateway] flusher not started: {e}", flush=True)

# --- Utils (required) --------------------------------------------------------
try:
    from utils import (
        info, warn, error,
        send_boot_notice_once, send_system_online_once, send_telegram_message_dedup,
        is_cold_boot,
        # token buckets & caches live inside utils wrappers
    )
except Exception as e:
    # If utils can't import, nothing else matters. Fail loud.
    print(f"[BOOT] FATAL: utils import failed: {e}")
    raise

# --- Telegram webhook / Flask (optional) -------------------------------------
RUN_WEBHOOK_IN_MAIN = (os.getenv("RUN_WEBHOOK_IN_MAIN", "0").strip().lower() in {"1","true","yes"})
_telegram_app = None

def _try_start_flask():
    """DEV ONLY: start a local Flask server; production uses gunicorn via wsgi.py."""
    global _telegram_app
    try:
        from flask import Flask
        try:
            from telegram_webhook import telegram_app, set_telegram_webhook
        except Exception as e:
            warn(f"telegram_webhook not available or failed to import: {e}")
            # Tiny fallback so local dev has a health endpoint
            telegram_app = Flask(__name__)
            def set_telegram_webhook():
                info("Skipping Telegram webhook (module missing).")
        _telegram_app = telegram_app
    except Exception as e:
        warn(f"Flask/telegram init failed: {e}")
        return  # bail out cleanly; don't continue if Flask isn't ready

    # --- Register Ops helper (enqueue) ---
    try:
        from ops_enqueue import bp as _ops_bp
        _telegram_app.register_blueprint(_ops_bp)
        info("✅ Ops helper registered at /ops/enqueue")
    except Exception as e:
        warn(f"Ops helper not registered: {e}")

    # --- Register Ops venue checker ---
    try:
        from ops_venue import bp as _ops_venue_bp
        _telegram_app.register_blueprint(_ops_venue_bp)
        info("✅ Ops venue checker at /ops/venue_check")
    except Exception as e:
        warn(f"Ops venue checker not registered: {e}")

    # --- Telegram webhook (best-effort, never fatal) ---
    info("Setting Telegram webhook…")
    try:
        set_telegram_webhook()
        info("✅ Telegram webhook configured.")
    except Exception as e:
        warn(f"Webhook setup skipped: {e}")

    # --- Only run the dev server when explicitly requested ---
    if os.getenv("RUN_FLASK_DEV", "0").strip().lower() in {"1", "true", "yes"}:
        port = int(os.getenv("PORT", "10000"))
        info(f"Starting Flask app on port {port}…")
        _telegram_app.run(host="0.0.0.0", port=port)

# --- Thread helper & jitter --------------------------------------------------
def _thread(fn: Callable, *a, **k):
    t = threading.Thread(target=fn, args=a, kwargs=k, daemon=True)
    t.start()
    return t

def _sleep_jitter(min_s=0.35, max_s=1.10):
    time.sleep(random.uniform(min_s, max_s))

# Start Flask + webhook (soft) — now gated, defaults OFF (Render uses gunicorn wsgi:app)
if RUN_WEBHOOK_IN_MAIN:
    _thread(_try_start_flask)

# --- Safe import + call helpers ----------------------------------------------
def _safe_import(module_path: str):
    """Import a module by string. Returns module or None."""
    try:
        __import__(module_path)
        return globals()[module_path] if module_path in globals() else __import__(module_path)
    except Exception as e:
        warn(f"Import skipped ({module_path}): {e}")
        return None

def _safe_call(label: str, module_path: str, func_name: str, *args, **kwargs):
    """Import module lazily and call func if present. Never raises."""
    try:
        mod = _safe_import(module_path)
        if not mod:
            return
        fn = getattr(mod, func_name, None)
        if not callable(fn):
            warn(f"{label}: {module_path}.{func_name} missing or not callable; skipping.")
            return
        info(f"▶ {label}")
        return fn(*args, **kwargs)
    except Exception as e:
        error(f"{label} failed: {e}")

def _schedule(label: str, module_path: str, func_name: str,
              when: Optional[str]=None, every: Optional[int]=None, unit: str="minutes"):
    """Add a scheduled job that safely imports & runs target each time."""
    def job():
        _sleep_jitter(0.2, 0.6)
        _safe_call(label, module_path, func_name)

    if when:
        schedule.every().day.at(when).do(job)
        info(f"⏰ Scheduled daily {label} at {when}")
    elif every:
        ev = getattr(schedule.every(every), unit)
        ev.do(job)
        info(f"⏰ Scheduled {label} every {every} {unit}")
    else:
        _thread(job)

# --- Optional background loop: staking yield (soft) --------------------------
def _staking_yield_loop():
    if os.getenv('ENABLE_STAKING_YIELD','0').lower() not in {'1','true','yes','on'}:
        info('staking_yield_tracker disabled by env.')
        return
    try:
        mod = _safe_import("staking_yield_tracker")
        fn = getattr(mod, "run_staking_yield_tracker", None) if mod else None
        if not callable(fn):
            info("staking_yield_tracker not present; loop disabled.")
            return
        while True:
            info("Checking staking yield…")
            try:
                fn()
            except Exception as e:
                warn(f"staking_yield_tracker error: {e}")
            time.sleep(6 * 3600)  # every 6h
    except Exception as e:
        warn(f"staking_yield loop not started: {e}")

# --- Phase 9 Imports (NovaTrade 3.0) -----------------------------------------
from rotation_feedback_enhancer import run_rotation_feedback_enhancer
from rotation_memory import run_rotation_memory  # wrapper; safe to keep
from rotation_signal_engine import run_milestone_alerts  # Days Held milestones
from rotation_executor import sync_confirmed_to_rotation_log  # header-safe Planner→Log
from council_ledger import ensure_ledger_tabs
from telemetry_digest import run_telemetry_digest
from unified_snapshot import run_unified_snapshot
from telemetry_mirror import _compact_wallet_monitor_if_needed

_schedule("Telemetry Digest", "telemetry_digest", "run_telemetry_digest", when="12:10")

# --- Boot orchestration ------------------------------------------------------
def _boot_serialize_first_minute():
    """
    Run the heaviest read/write jobs in a serialized, jittered order to
    minimize Sheets 429 bursts during cold boot. Best-effort; never raises.
    """
    _safe_call("Watchdog",                    "nova_watchdog",              "run_watchdog");                 _sleep_jitter()

    # Soft sheet health probe (optional)
    try:
        from utils import get_gspread_client
        SHEET_URL = os.getenv("SHEET_URL", "")
        if SHEET_URL:
            client = get_gspread_client()
            sh = client.open_by_url(SHEET_URL)
            _ = sh.worksheet("Presale_Stream")
            info("Presale_Stream loaded.")
    except Exception as e:
        warn(f"Presale_Stream load skipped: {e}")
    _sleep_jitter()

    _safe_call("ROI tracker (boot)",          "roi_tracker",                "scan_roi_tracking");            _sleep_jitter()
    # OLD milestone module removed (relied on 'Xd since vote' text)
    # _safe_call("Milestone alerts (boot)",   "milestone_alerts",           "run_milestone_alerts");         _sleep_jitter()
    _safe_call("Vault sync",                   "token_vault_sync",           "sync_token_vault");             _sleep_jitter()
    _safe_call("Top token summary",            "top_token_summary",          "run_top_token_summary");        _sleep_jitter()
    _safe_call("Vault intelligence",           "vault_intelligence",         "run_vault_intelligence");       _sleep_jitter()
    _safe_call("Vault rotation executor",      "vault_rotation_executor",    "run_vault_rotation_executor");  _sleep_jitter()
    _safe_call("Scout→Planner sync",           "scout_to_planner_sync",      "sync_rotation_planner");        _sleep_jitter()
    _safe_call("ROI feedback sync",            "roi_feedback_sync",          "run_roi_feedback_sync");        _sleep_jitter()

    _safe_call("Sentiment Radar (boot)",       "sentiment_radar",            "run_sentiment_radar");          _sleep_jitter()
    _safe_call("Nova trigger watcher",         "nova_trigger_watcher",       "check_nova_trigger");           _sleep_jitter()
    _safe_call("Nova ping",                    "nova_trigger_sender",        "trigger_nova_ping",);           _sleep_jitter()

    # --- Phase 9 additions (Planner→Log, Weighted Memory, Milestones) ---
    _safe_call("Planner→Log sync",             "rotation_executor",          "sync_confirmed_to_rotation_log"); _sleep_jitter()
    _safe_call("Rotation Memory (Weighted)",   "rotation_feedback_enhancer", "run_rotation_feedback_enhancer"); _sleep_jitter()
    _safe_call("Milestone Alerts (Days Held)", "rotation_signal_engine",     "run_milestone_alerts");           _sleep_jitter()
    _safe_call("Policy Bias Builder",          "policy_bias_engine",         "run_policy_bias_builder");        _sleep_jitter()

def _set_schedules():
    # Frequent cadence
    _schedule("Nova Trigger Watcher",          "nova_trigger_watcher",       "check_nova_trigger",       every=2, unit="minutes")
    _schedule("Rotation Log Updater",          "rotation_log_updater",       "run_rotation_log_updater", every=60, unit="minutes")
    _schedule("Rebalance Scanner",             "rebalance_scanner",          "run_rebalance_scanner",    every=60, unit="minutes")
    # Keep legacy wrapper scheduled if other modules rely on it; the enhancer now does the heavy lifting
    _schedule("Rotation Memory",               "rotation_memory",            "run_rotation_memory",     every=60, unit="minutes")
    _schedule("Sentiment Radar",               "sentiment_radar",            "run_sentiment_radar",     every=6,  unit="hours")
    _schedule("Memory-Aware Rebuy Scan",       "rebuy_memory_engine",        "run_memory_rebuy_scan",   every=3,  unit="hours")
    _schedule("Sentiment Summary",             "sentiment_summary",          "run_sentiment_summary",   every=3,  unit="hours")
    _schedule("Stalled Autotrader (Shadow)",     "stalled_autotrader",         "run_stalled_autotrader_shadow",    every=6,  unit="hours")
              
    # Daily (spread to avoid spikes)
    _schedule("Health Summary",                "health_summary",             "run_health_summary",          when="13:00")
    _schedule("ROI Threshold Validator",       "roi_threshold_validator",    "run_roi_threshold_validator", when="01:00")
    _schedule("Rebuy Weight Calculator",       "rebuy_weight_calculator",    "run_rebuy_weight_calculator", when="01:10")
    _schedule("Memory Score Sync",             "memory_score_sync",          "run_memory_score_sync",       when="01:15")
    _schedule("Top Token Summary",             "top_token_summary",          "run_top_token_summary",       when="01:30")
    _schedule("Vault ROI Tracker",             "vault_roi_tracker",          "run_vault_roi_tracker",       when="02:00")
    _schedule("Vault Rotation Scanner",        "vault_rotation_scanner",     "run_vault_rotation_scanner",  when="09:15")
    _schedule("Vault Rotation Executor",       "vault_rotation_executor",    "run_vault_rotation_executor", when="09:25")
    _schedule("Policy Bias Builder",           "policy_bias_engine",         "run_policy_bias_builder",     when="01:20") 
    _schedule("Rebuy ROI Tracker",             "rebuy_roi_tracker",          "run_rebuy_roi_tracker",       when="12:45")
    _schedule("Sentiment Alerts",              "sentiment_alerts",           "run_sentiment_alerts",        when="13:00")

        # --- Phase 9 added schedules ---
    _schedule("Planner→Log Sync",              "rotation_executor",          "sync_confirmed_to_rotation_log", every=30, unit="minutes")
    _schedule("Rotation Memory Weighted",      "rotation_feedback_enhancer", "run_rotation_feedback_enhancer", every=6,  unit="hours")
    _schedule("Milestone Alerts",              "rotation_signal_engine",     "run_milestone_alerts",           every=1,  unit="hours")
    _schedule("Wallet Monitor",                "wallet_monitor",             "run_wallet_monitor",             every=15, unit="minutes")
    _schedule("Telemetry Mirror",              "telemetry_mirror",           "mirror_telemetry_once",          every=15, unit="minutes")
    _schedule("Unified Snapshot",              "unified_snapshot",           "run_unified_snapshot",           every=15, unit="minutes")
    
def _kick_once_and_threads():
    # Background scheduler loop
    def _scheduler_loop():
        while True:
            try:
                schedule.run_pending()
            except Exception as e:
                warn(f"scheduler loop failed: {e}")
            time.sleep(1)

    _thread(_scheduler_loop)

    # Optional: one-time Telegram summary on boot (outside the loop)
    try:
        _safe_call("Telegram summaries (boot)", "telegram_summaries", "run_telegram_summaries")
    except Exception as e:
        warn(f"boot summary failed: {e}")

    # Stalled asset & claims (boot pass)
    _safe_call("Stalled asset detector (boot)", "stalled_asset_detector", "run_stalled_asset_detector"); _sleep_jitter()
    _safe_call("Claim tracker (boot)",          "claim_tracker",          "check_claims");               _sleep_jitter()
    _safe_call("Stalled Autotrader (Shadow)",   "stalled_autotrader",     "run_stalled_autotrader_shadow",); _sleep_jitter()
    # Staking yield background loop (optional)
    _thread(_staking_yield_loop);                                              _sleep_jitter()

    # Stats & memory syncs (threads + singles)
    _thread(_safe_call, "Rotation Stats Sync", "rotation_stats_sync", "run_rotation_stats_sync"); _sleep_jitter()
    _thread(_safe_call, "Memory Weight Sync",  "memory_weight_sync",  "run_memory_weight_sync");  _sleep_jitter()
    _safe_call("Total memory score",           "memory_score_sync",   "run_memory_score_sync");   _sleep_jitter()
    _thread(_safe_call, "Rebuy ROI Tracker",   "rebuy_roi_tracker",   "run_rebuy_roi_tracker");   _sleep_jitter()

    _safe_call("Rotation feedback engine",     "rotation_feedback_engine", "run_rotation_feedback_engine"); _sleep_jitter()

    # Performance dashboard (soft)
    info("Running Performance Dashboard…")
    _safe_call("Performance dashboard",        "performance_dashboard", "run_performance_dashboard")
    _sleep_jitter()

    # Initial rebalance scan
    info("Running initial rebalance scan…")
    _safe_call("Rebalance scan", "rebalance_scanner", "run_rebalance_scanner")
    _sleep_jitter()

    # Telegram summaries (soft)
    info("Running Telegram Summary Layer…")
    _safe_call("Telegram summaries", "telegram_summaries", "run_telegram_summaries")
    _sleep_jitter()
   
    # Memory & rebuy engines (one-shot)
    info("Running Rotation Memory Sync…")
    _safe_call("Rotation memory", "rotation_memory", "run_rotation_memory");                _sleep_jitter()

    info("Running undersized rebuy engine…")
    _safe_call("Undersized rebuy", "rebuy_engine", "run_undersized_rebuy");                 _sleep_jitter()

    info("Running memory-aware rebuy engine…")
    _safe_call("Memory aware rebuy", "rebuy_memory_engine", "run_memory_rebuy_scan");       _sleep_jitter()

    info("Calculating Rebuy Weights…")
    _safe_call("Rebuy weight calculator", "rebuy_weight_calculator", "run_rebuy_weight_calculator"); _sleep_jitter()

    info("Wallet Monitor…")
    _safe_call("Wallet Monitor", "wallet_monitor", "run_wallet_monitor"); _sleep_jitter()
    
    info("Running Unified Snapshot…")
    _safe_call("Unified Snapshot", "unified_snapshot", "run_unified_snapshot"); _sleep_jitter()

    info("Wallet Monitor Compactor…")
    _safe_call("Wallet Monitor Compactor", "telemetry_mirror", "_compact_wallet_monitor_if_needed"); _sleep_jitter()
               
    # Sentiment-triggered scan (soft)
    info("Sentiment-Triggered Rebuy Scan…")
    _safe_call("Sentiment trigger engine", "sentiment_trigger_engine", "run_sentiment_trigger_engine"); _sleep_jitter()

    # Memory scoring & target %
    _safe_call("Memory scoring", "rotation_memory_scoring", "run_memory_scoring");           _sleep_jitter()
    info("Running Suggested Target Calculator…")
    _safe_call("Portfolio weight adjuster", "portfolio_weight_adjuster", "run_portfolio_weight_adjuster"); _sleep_jitter()
    info("Syncing Suggested % → Target %…")
    _safe_call("Target % updater", "target_percent_updater", "run_target_percent_updater");  _sleep_jitter()

    # Vault flows
    info("Syncing Vault Tags → Rotation_Stats…")
    _thread(_safe_call, "Vault→Stats sync", "vault_to_stats_sync", "run_vault_to_stats_sync"); _sleep_jitter()

    _safe_call("Vault alerts", "vault_alerts_phase15d", "run_vault_alerts");                  _sleep_jitter()

    info("Vault ROI + Memory Stats sync…")
    _safe_call("Vault growth sync", "vault_growth_sync", "run_vault_growth_sync");            _sleep_jitter()

    info("Writing daily snapshot to Vault ROI Tracker…")
    _safe_call("Vault ROI tracker", "vault_roi_tracker", "run_vault_roi_tracker");            _sleep_jitter()

    info("Running Vault Review Alerts…")
    _safe_call("Vault review alerts", "vault_review_alerts", "run_vault_review_alerts");      _sleep_jitter()

    info("Scanning vaults for decay…")
    _safe_call("Vault rotation scanner", "vault_rotation_scanner", "run_vault_rotation_scanner"); _sleep_jitter()

    # Planner & unlock horizon
    info("Auto-Confirm Planner…")
    _safe_call("Auto-confirm planner", "auto_confirm_planner", "run_auto_confirm_planner");   _sleep_jitter()
    info("Auto-confirm check complete.")

    info("Unlock horizon alerts…")
    _safe_call("Unlock horizon alerts", "unlock_horizon_alerts", "run_unlock_horizon_alerts"); _sleep_jitter()

# --- Production-safe boot (no dev server) ------------------------------------
def boot():
    """Start all jobs/threads; expose Flask via wsgi.py/gunicorn (no app.run here)."""
    send_boot_notice_once("🟢 NovaTrade system booted and live.")

    # Ensure governance tabs before anything can log to them
    try:
        ensure_ledger_tabs()
    except Exception as e:
        warn(f"Ledger ensure skipped: {e}")

    time.sleep(0.4)
    _thread(_safe_call, "Orion Voice Loop", "orion_voice_loop", "run_orion_voice_loop")

    _boot_serialize_first_minute()
    send_system_online_once()
    _set_schedules()
    _kick_once_and_threads()
    send_telegram_message_dedup("✅ NovaTrade boot sequence complete.", key="boot_done")
    info("NovaTrade main loop running.")
    return True

# --- Dev entrypoint -----------------------------------------------------------
if __name__ == "__main__":
    # Do the same boot…
    boot()
    # …and start the dev Flask server in a thread for local testing.
    _thread(_try_start_flask)
    while True:
        time.sleep(5)

# --- WSGI app for Render (gunicorn points at main:app) -----------------------
try:
    # Try to reuse telegram app if available so both share one server
    from telegram_webhook import telegram_app as _flask_app
except Exception:
    from flask import Flask
    _flask_app = Flask(__name__)

app = _flask_app  # gunicorn loads this
