# rebalance_scanner.py — NT3.0 Phase-1 Polish (values-only reads, single batch, jitter)
import os, time, random
from utils import (
    get_ws, get_values_cached, ws_batch_update, with_sheet_backoff, str_or_empty
)

SRC_TAB = "Rotation_Stats"
DST_TAB = "Rotation_Planner"   # if you mark suggested actions there; adjust if different

TTL_READ_S   = int(os.getenv("REBAL_TTL_READ_SEC", "300"))  # cache 5m
MAX_WRITES   = int(os.getenv("REBAL_MAX_WRITES", "200"))    # cap per run
JITTER_MIN_S = float(os.getenv("REBAL_JITTER_MIN_S", "0.5"))
JITTER_MAX_S = float(os.getenv("REBAL_JITTER_MAX_S", "2.0"))

NEEDED_SRC = ["Token", "Target %", "Current %", "Suggested %"]  # adjust if yours differ
PLAN_HEADERS = ["Token", "Suggested Action", "Reason"]          # lightweight output

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _values_to_records(vals):
    if not vals: return []
    header = vals[0]
    out = []
    for row in vals[1:]:
        rec = { (header[i] if i < len(header) else f"__c{i+1}"): (row[i] if i < len(row) else "")
                for i in range(len(header)) }
        out.append(rec)
    return out

def _to_float(v, default=None):
    try:
        s = str(v).replace("%","").replace(",","").strip()
        return float(s) if s else default
    except Exception:
        return default

@with_sheet_backoff
def run_rebalance_scanner():
    print("▶️ Rebalance scanner …")
    # small jitter to de-sync from neighbors
    time.sleep(random.uniform(JITTER_MIN_S, JITTER_MAX_S))

    # --- reads (values-only; both cached)
    stats_vals = get_values_cached(SRC_TAB, ttl_s=TTL_READ_S) or []
    if not stats_vals:
        print("ℹ️ Rotation_Stats empty; skipping.")
        return
    planner_vals = get_values_cached(DST_TAB, ttl_s=TTL_READ_S) or []

    stats = _values_to_records(stats_vals)

    # ensure planner header and column map
    plan_header = planner_vals[0] if planner_vals else []
    changed_header = False
    for h in PLAN_HEADERS:
        if h not in plan_header:
            plan_header.append(h)
            changed_header = True
    plan_col = {h: i+1 for i, h in enumerate(plan_header)}

    # build planner existing token index (2-based row index)
    plan_token_col = plan_col.get("Token")
    planner_index = {}
    if plan_token_col:
        for i, row in enumerate(planner_vals[1:], start=2):
            tok = str_or_empty(row[plan_token_col-1] if plan_token_col-1 < len(row) else "").upper()
            if tok and tok not in planner_index:
                planner_index[tok] = i

    # scan for overweight/underweight
    suggestions = []  # (row_idx or None for append, [values…])
    writes = []
    if changed_header:
        writes.append({"range": f"A1:{_col_letter(len(plan_header))}1", "values": [plan_header]})

    touched = 0
    for r in stats:
        token = str_or_empty(r.get("Token")).upper()
        if not token:
            continue
        tgt = _to_float(r.get("Target %"))
        cur = _to_float(r.get("Current %"))
        sug = _to_float(r.get("Suggested %"))
        if tgt is None or cur is None or sug is None:
            continue

        # simple policy: if |cur - tgt| >= 1.0%, propose an action
        delta = cur - tgt
        if abs(delta) < 1.0:
            continue

        action = "SELL" if delta > 0 else "BUY"
        reason = f"Δ={delta:+.2f}pp (cur={cur:.2f}%, tgt={tgt:.2f}%)"

        # place/update in planner
        row = [""] * len(plan_header)
        row[plan_col["Token"]-1] = token
        row[plan_col["Suggested Action"]-1] = action
        row[plan_col["Reason"]-1] = reason

        if token in planner_index:
            row_idx = planner_index[token]
            writes.append({
                "range": f"A{row_idx}:{_col_letter(len(plan_header))}{row_idx}",
                "values": [row]
            })
        else:
            suggestions.append(row)

        touched += 1
        if touched >= MAX_WRITES:
            break

    if suggestions:
        start_row = (len(planner_vals) if planner_vals else 1) + 1
        writes.append({"range": f"A{start_row}", "values": suggestions})

    if writes:
        ws = get_ws(DST_TAB)
        ws_batch_update(ws, writes)
        print(f"✅ Rebalance scanner: wrote {touched} suggestion(s).")
    else:
        print("✅ Rebalance scanner: no changes needed.")
