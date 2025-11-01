# claim_tracker.py — NovaTrade 3.0 (Phase-6 Safe)
from datetime import datetime
import random, time, os
from utils import (
    get_ws_cached, get_values_cached, ws_batch_update, str_or_empty, warn
)

TAB = "Claim_Tracker"
READ_TTL = int(os.getenv("CLAIM_TTL_READ_SEC", "120"))
JIT_MIN  = float(os.getenv("CLAIM_JITTER_MIN_S", "0.4"))
JIT_MAX  = float(os.getenv("CLAIM_JITTER_MAX_S", "1.5"))
MAX_WRITES = int(os.getenv("CLAIM_MAX_WRITES", "500"))

def _a1(col_idx, row_idx):
    n, s = col_idx, ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return f"{s}{row_idx}"

def _first_present(header, *names):
    for n in names:
        if n in header:
            return n
    return None

def _boolish(v) -> bool:
    s = str_or_empty(v).lower()
    return s in {"true", "yes", "y", "1", "claimed", "✅", "done"}

def check_claims():
    try:
        time.sleep(random.uniform(JIT_MIN, JIT_MAX))
        vals = get_values_cached(TAB, ttl_s=READ_TTL)
        if not vals:
            return
        hdr, rows = vals[0], vals[1:]
        h_claimable = _first_present(hdr, "Claimable", "Is Claimable", "Ready")
        h_claimed   = _first_present(hdr, "Claimed?", "Claimed")
        h_status    = _first_present(hdr, "Status")
        h_unlock    = _first_present(hdr, "Unlock Date", "Unlock_Date", "Unlock At")
        h_days      = _first_present(hdr, "Days Since Unlock", "Days", "Days_Since")
        idx = {n: i + 1 for i, n in enumerate(hdr)}

        writes = []
        now = datetime.now()
        for rnum, row in enumerate(rows, start=2):
            def _get(name):
                if not name: return ""
                ci = idx.get(name, 0) - 1
                return row[ci] if 0 <= ci < len(row) else ""
            claimable = str_or_empty(_get(h_claimable)).upper() == "TRUE" if h_claimable else False
            claimed   = _boolish(_get(h_claimed)) if h_claimed else False
            days_val, status_val = "", ""

            if h_unlock:
                raw = str_or_empty(_get(h_unlock))
                if raw:
                    try:
                        dt = datetime.strptime(raw, "%Y-%m-%d")
                        days_val = "" if claimed else str((now - dt).days)
                    except Exception:
                        pass

            if claimed: status_val = "✅ Claimed"
            elif claimable: status_val = "Claim Now"

            if h_days:
                writes.append({"range": f"{TAB}!{_a1(idx[h_days], rnum)}", "values": [[days_val]]})
            if h_status:
                writes.append({"range": f"{TAB}!{_a1(idx[h_status], rnum)}", "values": [[status_val]]})
            if len(writes) >= MAX_WRITES:
                break

        if writes:
            ws = get_ws_cached(TAB, ttl_s=60)
            ws_batch_update(ws, writes)
    except Exception as e:
        warn(f"claim_tracker: {e}")

run_claim_tracker = check_claims
