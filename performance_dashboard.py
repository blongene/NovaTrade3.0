# performance_dashboard.py — NovaTrade 3.0 (Phase-1 Polish)
# Compact KPI snapshot → Performance_Dashboard (atomic write)

from statistics import median
from datetime import datetime
from utils import (
    get_ws,
    get_records_cached,
    str_or_empty,
    to_float,
    with_sheet_backoff,
)

def run_performance_dashboard():
    print("📊 Running Performance Dashboard …")
    try:
        stats = get_records_cached("Rotation_Stats", ttl_s=180) or []
        hb_ws = get_ws("NovaHeartbeat")
        dash_ws = get_ws("Performance_Dashboard")

        total = 0
        yes_count = 0
        yes_perf = []
        wins_yes = 0

        for r in stats:
            token = str_or_empty(r.get("Token"))
            if not token:
                continue
            total += 1

            decision = str_or_empty(r.get("Decision")).upper()
            perf_val = to_float(r.get("Performance"))

            if decision == "YES":
                yes_count += 1
                if perf_val is not None:
                    yes_perf.append(perf_val)
                    if perf_val > 0:
                        wins_yes += 1

        win_rate = (wins_yes / yes_count * 100.0) if yes_count else 0.0
        med_yes  = median(yes_perf) if yes_perf else 0.0

        # Heartbeat pull
        try:
            last_hb = str_or_empty(hb_ws.acell("A2").value)
        except Exception:
            last_hb = ""

        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        values = [
            ["NovaTrade Performance Dashboard", ""],
            ["Last Dashboard Refresh", now_str],
            ["Last Heartbeat (A2)", last_hb],
            ["", ""],
            ["Total Tokens", total],
            ["YES Count", yes_count],
            ["Win Rate (YES, %positive Performance)", f"{win_rate:.2f}%"],
            ["Median Performance (YES)", f"{med_yes:.2f}"],
        ]

        dash_ws.update("A1:B8", values, value_input_option="RAW")
        print("✅ performance_dashboard: updated successfully.")

    except Exception as e:
        print(f"❌ performance_dashboard error: {e}")

if __name__ == "__main__":
    run_performance_dashboard()
