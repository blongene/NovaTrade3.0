# telegram_summaries.py
import os
from datetime import datetime, timezone

from utils import (
    with_sheet_backoff,
    get_sheet,
    send_once_per_day,
)

# --------- helpers (429/backoff-wrapped) ---------

@with_sheet_backoff
def _ws_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _ws_value(ws, a1):
    # minimal cell read; if it fails, caller handles fallback
    return ws.acell(a1).value

def _utc_datestr():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

# --------- data collection ---------

def _collect_counts():
    """Return (pending_rotations, yes, no, skip) with minimal reads."""
    sh = get_sheet()

    # Rotation_Planner: count 'pending' as rows where Confirmed is blank/falsey
    try:
        planner_ws = sh.worksheet("Rotation_Planner")
        planner = _ws_records(planner_ws)
        pending = 0
        for r in planner:
            confirmed = (r.get("Confirmed") or "").strip().upper()
            if confirmed in ("", "NO", "PENDING"):
                pending += 1
    except Exception:
        pending = 0

    # Rotation_Stats (or Scout Decisions) for feedback tallies.
    # Prefer Rotation_Stats if it has clear columns; fall back to Scout Decisions.
    yes = no = skip = 0
    counted = False
    try:
        stats_ws = sh.worksheet("Rotation_Stats")
        stats = _ws_records(stats_ws)
        for r in stats:
            d = (r.get("Decision") or "").strip().upper()
            if d == "YES":
                yes += 1
                counted = True
            elif d == "NO":
                no += 1
                counted = True
            elif d == "SKIP":
                skip += 1
                counted = True
    except Exception:
        counted = False

    if not counted:
        # Fallback to Scout Decisions (column 3 is usually decision)
        try:
            scout_ws = sh.worksheet("Scout Decisions")
            scout = _ws_records(scout_ws)
            for r in scout:
                d = (r.get("Decision") or r.get("Action") or "").strip().upper()
                if d == "YES":
                    yes += 1
                elif d == "NO":
                    no += 1
                elif d == "SKIP":
                    skip += 1
        except Exception:
            pass

    return pending, yes, no, skip

def _build_summary_html():
    date_utc = _utc_datestr()
    pending, yes, no, skip = _collect_counts()

    # You can expand this later; keep it concise to avoid long logs/telegrams.
    html = (
        f"🧾 <b>Daily Summary — {date_utc} (UTC)</b>\n"
        f"⏳ Pending Rotations: {pending}\n"
        f"🗳 Feedback — YES:{yes}  NO:{no}  SKIP:{skip}"
    )
    return html

# --------- public entry point ---------

def run_telegram_summary():
    """
    Build + send the daily summary ONCE per UTC day.
    Uses utils.send_once_per_day('daily_summary', ...).
    """
    try:
        msg = _build_summary_html()
        sent = send_once_per_day("daily_summary", msg)
        if sent:
            print("✅ Telegram daily summary sent.")
        else:
            print("🔇 Telegram daily summary suppressed (already sent today).")
    except Exception as e:
        # Keep this quiet to avoid spam; one compact line is enough.
        print(f"❌ telegram summary error: {e}")
