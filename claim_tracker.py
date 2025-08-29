# claim_tracker.py — NovaTrade 3.0 (Phase-1 Polish)
# - Single cached read from Claim_Tracker
# - Batched writes (Status + Days Since Unlock)
# - Header fuzzing for resilience
# - Backward-compat: run_claim_tracker() -> check_claims()

from datetime import datetime
from utils import (
    get_ws, get_values_cached, ws_batch_update, str_or_empty
)

# ===== helpers =====
def _a1(col_idx: int, row_idx: int) -> str:
    n = col_idx
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return f"{letters}{row_idx}"

def _first_present(header: list[str], *names):
    for n in names:
        if n in header:
            return n
    return None

def _boolish(v) -> bool:
    s = str_or_empty(v).lower()
    return s in {"true", "yes", "y", "1", "claimed", "✅", "done"}

# ===== main =====
def check_claims():
    # One cached read
    vals = get_values_cached("Claim_Tracker", ttl_s=120)
    if not vals:
        return
    hdr = vals[0]
    rows = vals[1:]

    # Fuzzy headers
    h_claimable = _first_present(hdr, "Claimable", "Is Claimable", "Ready")
    h_claimed   = _first_present(hdr, "Claimed?", "Claimed")
    h_status    = _first_present(hdr, "Status")
    h_unlock    = _first_present(hdr, "Unlock Date", "Unlock_Date", "Unlock At")
    h_days      = _first_present(hdr, "Days Since Unlock", "Days", "Days_Since")

    # Build header -> 1-based col index
    idx = {name: i + 1 for i, name in enumerate(hdr)}

    writes = []
    now = datetime.now()

    for rnum, row in enumerate(rows, start=2):
        def _get(name):
            if not name:
                return ""
            ci = idx.get(name, 0) - 1
            return row[ci] if 0 <= ci < len(row) else ""

        claimable = str_or_empty(_get(h_claimable)).upper() == "TRUE" if h_claimable else False
        claimed   = _boolish(_get(h_claimed)) if h_claimed else False

        # Days since unlock
        days_val = ""
        if h_unlock:
            raw = str_or_empty(_get(h_unlock))
            if raw:
                try:
                    dt = datetime.strptime(raw, "%Y-%m-%d")
                    days_val = "" if claimed else str((now - dt).days)
                except Exception:
                    days_val = ""

        # Status
        if claimed:
            status_val = "✅ Claimed"
        elif claimable:
            status_val = "Claim Now"
        else:
            status_val = ""

        if h_days:
            writes.append({"range": f"Claim_Tracker!{_a1(idx[h_days], rnum)}", "values": [[days_val]]})
        if h_status:
            writes.append({"range": f"Claim_Tracker!{_a1(idx[h_status], rnum)}", "values": [[status_val]]})

    if writes:
        ws = get_ws("Claim_Tracker")
        ws_batch_update(ws, writes)  # single round-trip

# Backward-compat entry point
def run_claim_tracker():
    return check_claims()

if __name__ == "__main__":
    check_claims()
