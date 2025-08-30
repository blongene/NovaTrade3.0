# nova_trigger_watcher.py — cache-first + zero chatter
import os, time, random
from utils import get_values_cached, with_sheet_backoff, str_or_empty

TAB = os.getenv("NOVA_TRIGGER_TAB", "NovaTrigger")   # simple 2-cell sheet
TTL = int(os.getenv("NOVA_TRIGGER_TTL_SEC", "120"))  # cache 2m
JIT_MIN = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S", "0.3"))
JIT_MAX = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S", "1.2"))

@with_sheet_backoff
def check_nova_trigger():
    print("▶ Nova trigger check …")
    time.sleep(random.uniform(JIT_MIN, JIT_MAX))  # de-sync from neighbors

    # Values-only, single read (then TTL-cached by utils)
    vals = get_values_cached(TAB, ttl_s=TTL) or []
    if not vals:
        print(f"ℹ️ {TAB} empty; no trigger.")
        return False, ""

    # Minimal schema:
    # A1 = "Enabled" / "On" / "1" to fire
    # B1 = optional message
    flag = str_or_empty(vals[0][0] if vals[0] else "").strip().lower()
    msg  = str_or_empty(vals[0][1] if len(vals[0]) > 1 else "")

    is_on = flag in {"1","on","true","enabled","yes","y"}
    if is_on:
        print("✅ NovaTrigger ON")
        return True, msg
    print("✅ NovaTrigger OFF")
    return False, ""
