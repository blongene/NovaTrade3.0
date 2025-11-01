# performance_dashboard.py — Phase-6 Safe
import os, random, time
from utils import (
    get_values_cached, get_ws_cached, ws_update,
    sheets_gate, warn, info
)

SRC_TAB  = os.getenv("PERF_SRC_TAB", "Rotation_Stats")
DEST_TAB = os.getenv("PERF_DEST_TAB", "Performance_Dashboard")
READ_TTL = int(os.getenv("PERF_READ_TTL_SEC", "300"))
JIT_MIN  = float(os.getenv("PERF_JITTER_MIN_S", "0.3"))
JIT_MAX  = float(os.getenv("PERF_JITTER_MAX_S", "1.0"))

def run_performance_dashboard():
    try:
        time.sleep(random.uniform(JIT_MIN, JIT_MAX))
        vals = get_values_cached(SRC_TAB, ttl_s=READ_TTL)
        if not vals:
            warn(f"{SRC_TAB} empty; skip.")
            return
        # build your actual dashboard table here:
        rows_2d = [["Metric","Value"],["Example","OK"]]
        with sheets_gate("write"):
            ws = get_ws_cached(DEST_TAB, ttl_s=60)
            ws_update(ws,"A1",rows_2d)
        info("performance_dashboard updated.")
    except Exception as e:
        warn(f"Dashboard skipped: {e}")
