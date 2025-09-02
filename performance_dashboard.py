# performance_dashboard.py — keep your logic; just cache all reads + gate writes
import os, random, time
from utils import (
    get_values_cached, ws_update, with_sheet_backoff, sheets_gate, warn, info
)

SRC_TAB  = os.getenv("PERF_SRC_TAB", "Rotation_Stats")
DEST_TAB = os.getenv("PERF_DEST_TAB", "Performance_Dashboard")
READ_TTL = int(os.getenv("PERF_READ_TTL_SEC", "300"))   # 5m cache window
JIT_MIN  = float(os.getenv("PERF_JITTER_MIN_S", "0.3"))
JIT_MAX  = float(os.getenv("PERF_JITTER_MAX_S", "1.0"))

@with_sheet_backoff
def _open_ws(title):
    from utils import get_ws_cached
    return get_ws_cached(title, ttl_s=60)

def run_performance_dashboard():
    try:
        time.sleep(random.uniform(JIT_MIN, JIT_MAX))
        vals = get_values_cached(SRC_TAB, ttl_s=READ_TTL) or []
        if not vals:
            warn(f"Performance Dashboard: {SRC_TAB} empty; skipping.")
            return

        # … do your in-memory computations …
        #   build `rows_2d` = [[header...], [data...], ...]

        # EXAMPLE placeholder (replace with your real computed table):
        rows_2d = [["Metric", "Value"], ["Example", "OK"]]

        with sheets_gate("write", tokens=1):
            ws = _open_ws(DEST_TAB)
            ws_update(ws, "A1", rows_2d)

        info("performance_dashboard: updated.")
    except Exception as e:
        warn(f"Dashboard skipped (quota or parse): {e}")
