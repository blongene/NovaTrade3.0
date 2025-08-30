# performance_dashboard.py — NT3.0 Phase-1 Polish (ultra-lean)
from statistics import median
from datetime import datetime
from utils import (
    get_ws, get_records_cached, str_or_empty, to_float, with_sheet_backoff
)

TAB = "Performance_Dashboard"

@with_sheet_backoff
def run_performance_dashboard():
    print("📊 Running Performance Dashboard …")
    stats = get_records_cached("Rotation_Stats", ttl_s=300) or []
    hb_ok, last_hb = False, ""

    # Try to read heartbeat very cheaply (no per-cell loops)
    try:
        hb_ws = get_ws("NovaHeartbeat")
        last_hb = hb_ws.acell("A2").value or ""
        hb_ok = True
    except Exception:
        pass

    total = yes_count = wins_yes = 0
    yes_perf = []
    for r in stats:
        token = str_or_empty(r.get("Token"))
        if not token:
            continue
        total += 1
        if str_or_empty(r.get("Decision")).upper() == "YES":
            yes_count += 1
            p = to_float(r.get("Performance"))
            if p is not None:
                yes_perf.append(p)
                if p > 0:
                    wins_yes += 1

    win_rate = (wins_yes / yes_count * 100.0) if yes_count else 0.0
    med_yes  = median(yes_perf) if yes_perf else 0.0
    now_str  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    values = [
        ["NovaTrade Performance Dashboard", ""],
        ["Last Dashboard Refresh", now_str],
        ["Last Heartbeat (A2)" if hb_ok else "Last Heartbeat (A2) [unavailable]", last_hb],
        ["", ""],
        ["Total Tokens", total],
        ["YES Count", yes_count],
        ["Win Rate (YES, %positive Performance)", f"{win_rate:.2f}%"],
        ["Median Performance (YES)", f"{med_yes:.2f}"],
    ]

    ws = get_ws(TAB)
    ws.update("A1:B8", values, value_input_option="RAW")
    print("✅ performance_dashboard: updated.")
