# claim_tracker.py — NT3.0 Phase-1 polish (quota-proof, batched writes)
from datetime import datetime
from utils import (
    get_ws, get_values_cached, ws_batch_update,
    str_or_empty
)

def _a1(col_idx: int, row_idx: int) -> str:
    # 1-based col -> letters
    n = col_idx
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return f"{letters}{row_idx}"

def _first_present(hdr: list[str], *names):
    for name in names:
        if name in hdr:
            return name
    return None

def _parse_boolish(v):
    s = str_or_empty(v).strip().lower()
    return s in {"true", "yes", "y", "1", "claimed", "✅"}

def check_claims():
    # Single cached read
    vals = get_values_cached("Claim_Tracker", ttl_s=120)
    if not vals:
        return
    hdr = vals[0]
    rows = vals[1:]

    # Header mapping
    # Try to be tolerant to small header variations
    col_claimable = _first_present(hdr, "Claimable", "Is Claimable")
    col_claimed   = _first_present(hdr, "Claimed?", "Claimed")
    col_status    = _first_present(hdr, "Status")
    col_unlock    = _first_present(hdr, "Unlock Date", "Unlock_Date", "Unlock At")
    col_days      = _first_present(hdr, "Days Since Unlock", "Days", "Days_Since")

    # If critical headers are missing, just exit quietly
    if not (col_claimable and col_claimed and col_status and col_days):
        # We can compute days only if unlock date header exists
        pass

    # Convert header names -> 1-based column indexes
    idx = {name: i+1 for i, name in enumerate(hdr)}

    writes = []

    now = datetime.now()
    for r_index, row in enumerate(rows, start=2):  # sheet row number
        # Read fields via safe indexing
        def _get(name):
            try:
                return row[idx[name]-1] if idx.get(name, 0) - 1 < len(row) else ""
            except Exception:
                return ""

        claimable = str_or_empty(_get(col_claimable)).upper() == "TRUE" if col_claimable else False
        claimed   = _parse_boolish(_get(col_claimed)) if col_claimed else False
        status_new = ""
        days_val   = ""

        # Compute days since unlock if we have a date and not claimed
        if col_unlock:
            unlock_raw = str_or_empty(_get(col_unlock))
            if unlock_raw:
                try:
                    unlock_dt = datetime.strptime(unlock_raw, "%Y-%m-%d")
                    if not claimed:
                        days_val = str((now - unlock_dt).days)
                    else:
                        days_val = ""  # clear when claimed
                except Exception:
                    # leave days_val blank on parse issues
                    days_val = ""

        # Status column
        if claimable and not claimed:
            status_new = "Claim Now"
        elif claimed:
            status_new = "✅ Claimed"
        else:
            status_new = ""

        # Stage cell writes only when we actually have a target column
        if col_days:
            writes.append({
                "range": f"Claim_Tracker!{_a1(idx[col_days], r_index)}",
                "values": [[days_val]]
            })
        if col_status:
            writes.append({
                "range": f"Claim_Tracker!{_a1(idx[col_status], r_index)}",
                "values": [[status_new]]
            })

    # Single round-trip
    if writes:
        ws = get_ws("Claim_Tracker")
        ws_batch_update(ws, writes)
