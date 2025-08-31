# telegram_summaries.py — NT3.0 safe daily summary (quiet by default)
import os
from datetime import datetime, timezone

from utils import (
    send_telegram_message_dedup,
    tg_should_send,
    get_all_records_cached,
    get_value_cached,
    detect_stalled_tokens,
    warn, info, str_or_empty
)

# Env toggles
ENABLED = (os.getenv("TELEGRAM_SUMMARIES_ENABLED", "1") in ("1", "true", "True"))
DEDUP_TTL_MIN = int(os.getenv("TELEGRAM_SUMMARIES_TTL_MIN", "1440"))  # 24h
SUMMARY_KEY_BASE = os.getenv("TELEGRAM_SUMMARY_KEY_BASE", "telegram_summary_daily")

# Optional sheet/tab names (best-effort; code never crashes if missing)
ROTATION_LOG_TAB   = os.getenv("ROTATION_LOG_TAB", "Rotation_Log")
ROTATION_STATS_TAB = os.getenv("ROTATION_STATS_TAB", "Rotation_Stats")

def _utc_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _best_effort_counts():
    """Try to compute a couple of simple metrics; always safe, never raises."""
    counts = {}
    try:
        rows = get_all_records_cached(ROTATION_LOG_TAB, ttl_s=60)
        counts["rotation_rows"] = len(rows)
    except Exception as e:
        warn(f"telegram_summaries: Rotation_Log read failed: {e}")
        counts["rotation_rows"] = "—"

    # stalled tokens (watchdog)
    try:
        stalled = detect_stalled_tokens()
        counts["stalled"] = len(stalled)
    except Exception as e:
        warn(f"telegram_summaries: stalled detector failed: {e}")
        counts["stalled"] = "—"

    # Optional heartbeat cell if you keep one (won't crash if absent)
    try:
        hb = str_or_empty(get_value_cached("NovaHeartbeat", "A2", ttl_s=60))
        counts["heartbeat"] = hb or "—"
    except Exception:
        counts["heartbeat"] = "—"

    return counts

def _format_message(counts):
    today = _utc_date()
    lines = [
        f"📊 *NovaTrade Daily Summary* — {today} (UTC)",
        f"• Rotation_Log rows: {counts.get('rotation_rows', '—')}",
        f"• Stalled tokens (≥ threshold): {counts.get('stalled', '—')}",
    ]
    hb = counts.get("heartbeat", "—")
    if hb != "—":
        lines.append(f"• Heartbeat: {hb}")
    lines.append("—")
    lines.append("This is an automated status ping. Set TELEGRAM_SUMMARIES_ENABLED=0 to disable.")
    return "\n".join(lines)

def run_telegram_summaries(force: bool = False):
    """
    Called by main.py. Safe no-op if disabled.
    De-duped to 1x/day by default. Use force=True to bypass de-dupe.
    """
    if not ENABLED:
        info("telegram_summaries: disabled via TELEGRAM_SUMMARIES_ENABLED=0 (no-op).")
        return

    key = f"{SUMMARY_KEY_BASE}:{_utc_date()}"
    if not force and not tg_should_send("daily_summary", key=key, ttl_min=DEDUP_TTL_MIN, consume=True):
        # Already sent today; stay quiet
        return

    try:
        counts = _best_effort_counts()
        msg = _format_message(counts)
        # Use a fixed dedup key so accidental double-calls won't spam
        send_telegram_message_dedup(msg, key="telegram_summary_daily", ttl_min=DEDUP_TTL_MIN)
        info("telegram_summaries: summary sent.")
    except Exception as e:
        # Never crash boot due to summary issues
        warn(f"telegram_summaries: failed to send summary: {e}")
