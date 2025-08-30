# auto_confirm_planner.py — NT3.0 Phase-1 Polish
# Reads Rotation_Planner once (cached), writes only changed "Confirmed" cells in one batch.

import os, time, random
from utils import get_values_cached, get_ws, ws_batch_update, with_sheet_backoff, str_or_empty

TAB = os.getenv("ROT_PLANNER_TAB", "Rotation_Planner")

TTL_READ_S   = int(os.getenv("ACP_TTL_READ_SEC", "300"))   # cache 5m
MAX_WRITES   = int(os.getenv("ACP_MAX_WRITES", "400"))     # safety cap per run
JITTER_MIN_S = float(os.getenv("ACP_JITTER_MIN_S", "0.4"))
JITTER_MAX_S = float(os.getenv("ACP_JITTER_MAX_S", "1.6"))

YES_SET = { "YES", "Y", "TRUE", "ON", "APPROVE", "APPROVED" }

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _find_col(header, *names):
    for name in names:
        if name in header:
            return header.index(name)
    return None

@with_sheet_backoff
def run_auto_confirm_planner():
    print("📋 Running Auto-Confirm for Rotation_Planner...")
    time.sleep(random.uniform(JITTER_MIN_S, JITTER_MAX_S))  # de-sync with neighbors

    vals = get_values_cached(TAB, ttl_s=TTL_READ_S) or []
    if not vals:
        print("ℹ️ Rotation_Planner empty; skipping.")
        return

    header = vals[0]
    # Flexible column detection (case-sensitive to match Sheet headers)
    tok_c = _find_col(header, "Token")
    resp_c = _find_col(header, "User Response", "Response")
    deci_c = _find_col(header, "Decision")  # optional fallback
    conf_c = _find_col(header, "Confirmed")

    if tok_c is None or conf_c is None or (resp_c is None and deci_c is None):
        missing = []
        if tok_c is None:  missing.append("Token")
        if conf_c is None: missing.append("Confirmed")
        if resp_c is None and deci_c is None: missing.append("User Response/Decision")
        print(f"⚠️ Missing columns in {TAB}: {', '.join(missing)}; skipping.")
        return

    writes = []
    touched = 0

    # Scan rows and stage only cells that need "YES"
    for r_idx, row in enumerate(vals[1:], start=2):
        token = str_or_empty(row[tok_c] if tok_c < len(row) else "").upper()
        if not token:
            continue

        cur_conf = str_or_empty(row[conf_c] if conf_c < len(row) else "").upper()
        if cur_conf in YES_SET:
            continue  # already confirmed

        # Prefer explicit "User Response"; fall back to "Decision" if not present
        user_resp = str_or_empty(row[resp_c] if (resp_c is not None and resp_c < len(row)) else "").upper()
        decision  = str_or_empty(row[deci_c] if (deci_c is not None and deci_c < len(row)) else "").upper()

        should_confirm = (user_resp in YES_SET) or (user_resp == "" and decision in YES_SET)
        if not should_confirm:
            continue

        cell_a1 = f"{_col_letter(conf_c + 1)}{r_idx}"
        writes.append({"range": cell_a1, "values": [["YES"]]})
        touched += 1
        if touched >= MAX_WRITES:
            break

    if not writes:
        print("✅ Auto-confirm: no changes needed.")
        return

    ws = get_ws(TAB)  # open only when we actually write
    ws_batch_update(ws, writes)
    print(f"✅ Auto-confirm: updated {touched} row(s).")
