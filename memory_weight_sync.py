# memory_weight_sync.py — quota-calm + clear diagnostics
import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff, str_or_empty
)

TAB = os.getenv("MEM_WEIGHT_TAB", "Rotation_Stats")   # where both source & dest live
TTL = int(os.getenv("MEM_WEIGHT_TTL_SEC", "300"))
JIT_MIN = float(os.getenv("MEM_WEIGHT_JITTER_MIN_S", "0.4"))
JIT_MAX = float(os.getenv("MEM_WEIGHT_JITTER_MAX_S", "1.2"))

# Columns (change here if your headers differ)
SRC_SCORE_COL  = os.getenv("MEM_SCORE_COL", "Memory Score")   # read
DEST_PCT_COL   = os.getenv("MEM_DEST_COL",  "Suggested %")    # write

MIN_UPDATE_DIFF = float(os.getenv("MEM_MIN_DIFF_PP", "0.01")) # ignore tiny diffs
CREATE_ROWS_IF_MISSING = os.getenv("MEM_CREATE_ROWS", "true").lower() == "true"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _to_float(v, default=None):
    try:
        s = str(v).replace("%","").replace(",","").strip()
        return float(s) if s else default
    except Exception:
        return default

@with_sheet_backoff
def run_memory_weight_sync():
    print("🔁 Syncing Memory Weights...")
    time.sleep(random.uniform(JIT_MIN, JIT_MAX))

    vals = get_values_cached(TAB, ttl_s=TTL) or []
    if not vals:
        print(f"ℹ️ {TAB} empty; skipping.")
        return

    header = vals[0]
    colmap = {h: i+1 for i, h in enumerate(header)}
    missing_cols = [c for c in ("Token", SRC_SCORE_COL, DEST_PCT_COL) if c not in colmap]
    if missing_cols:
        print(f"ℹ️ Missing required column(s) in {TAB}: {', '.join(missing_cols)}; skipping.")
        return

    tok_c  = colmap["Token"] - 1
    src_c  = colmap[SRC_SCORE_COL] - 1
    dst_c  = colmap[DEST_PCT_COL] - 1

    # Build token -> (score, current_dest) map and remember empty rows
    tokens = []
    rows   = vals[1:]
    for r in rows:
        token = str_or_empty(r[tok_c] if tok_c < len(r) else "").upper()
        if not token:
            continue
        score = _to_float(r[src_c] if src_c < len(r) else None)
        dest  = _to_float(r[dst_c] if dst_c < len(r) else None)
        tokens.append((token, score, dest))

    if not tokens:
        print("ℹ️ No token rows found; skipping.")
        return

    # Convert score → weight %. Replace with your actual policy.
    def score_to_percent(score):
        if score is None: return None
        # Example linear map: 0..100 → 0..10% (tune!)
        return max(0.0, min(10.0, float(score) * 0.10))

    writes, to_create = [], []
    for idx, (token, score, dest) in enumerate(tokens, start=2):
        target_pct = score_to_percent(score)
        if target_pct is None:
            continue
        if dest is not None and abs(dest - target_pct) < MIN_UPDATE_DIFF:
            continue
        # Prepare single-cell write
        cell = f"{_col_letter(dst_c+1)}{idx}"
        writes.append({"range": cell, "values": [[f"{target_pct}"]]})

    # If destination rows are missing entirely, optionally create them
    # (Only relevant if your DEST_PCT_COL is on a different tab. For same-tab we don’t add rows.)
    if to_create and CREATE_ROWS_IF_MISSING:
        start = len(vals) + 1
        writes.append({"range": f"A{start}", "values": to_create})

    if not writes:
        print("✅ Memory weight sync: no changes needed.")
        return

    ws = get_ws(TAB)
    ws_batch_update(ws, writes)
    print(f"✅ Memory weight sync: updated {len(writes)} cell/range(s).")
