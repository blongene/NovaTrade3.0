# rebuy_weight_calculator.py — NT3.0 Render (429-safe, batch-only)
# Purpose: compute per-token "Rebuy Weight" from Rotation_Stats inputs in ONE pass.
# I/O pattern:
#   - ONE cached read of Rotation_Stats (values-only)
#   - ONE batched write for changed cells (auto-creates output column)
# Notes:
#   - All column names and behavior are ENV-tunable (see config block).
#   - Designed to be resilient to missing columns; it will skip gracefully.

import os, time, random, math
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty, to_float
)

# ===================== Config (override via ENV in Render) ====================

STATS_TAB          = os.getenv("ROT_STATS_TAB", "Rotation_Stats")

# Inputs (any missing ones are optional; the calc degrades gracefully)
COL_TOKEN          = os.getenv("ROT_STATS_TOKEN_COL", "Token")
COL_MEMORY_SCORE   = os.getenv("REBUY_COL_MEMORY_SCORE", "Total Memory Score")  # or "Memory Score"
COL_PERFORMANCE    = os.getenv("REBUY_COL_PERF", "Performance")                 # optional (e.g., ROI%)
COL_DAYS_HELD      = os.getenv("REBUY_COL_DAYS", "Days Held")                   # optional
COL_STATUS         = os.getenv("REBUY_COL_STATUS", "Status")                    # optional
COL_TAGS           = os.getenv("REBUY_COL_TAGS", "Memory Tag")                  # optional

# Output
COL_OUTPUT         = os.getenv("REBUY_COL_OUTPUT", "Rebuy Weight")              # auto-created if missing

# Behavior knobs
TTL_S              = int(os.getenv("REBUY_WEIGHT_TTL_SEC", "300"))
JIT_MIN_S          = float(os.getenv("REBUY_WEIGHT_JITTER_MIN_S", "0.35"))
JIT_MAX_S          = float(os.getenv("REBUY_WEIGHT_JITTER_MAX_S", "1.10"))
MAX_WRITES         = int(os.getenv("REBUY_WEIGHT_MAX_WRITES", "1000"))

# Weight formula knobs
#   base = max(memory_score, 0)
#   boost for tags containing any of TOKENS_BOOST (case-insensitive substring match)
#   decay by days_held (soft) and/or by poor performance (soft), if those columns exist
TOKENS_BOOST       = [s.strip().upper() for s in os.getenv("REBUY_BOOST_TOKENS", "REBUY,BUYBACK,SIGNAL").split(",")]
BOOST_MULT         = float(os.getenv("REBUY_BOOST_MULT", "1.15"))

ACTIVE_ONLY        = os.getenv("REBUY_ACTIVE_ONLY", "false").lower() == "true"  # if true, skip rows where Status != "Active"
ACTIVE_VALUE       = os.getenv("REBUY_ACTIVE_VALUE", "Active").upper()

DAYS_DECAY_START   = int(os.getenv("REBUY_DAYS_DECAY_START", "10"))   # begin decaying after N days
DAYS_DECAY_RATE    = float(os.getenv("REBUY_DAYS_DECAY_RATE", "0.02"))# per day beyond start, multiplicative
PERF_SOFT_FLOOR    = float(os.getenv("REBUY_PERF_SOFT_FLOOR", "-20")) # if Performance < floor, reduce weight
PERF_DECAY_RATE    = float(os.getenv("REBUY_PERF_DECAY_RATE", "0.01"))# multiplicative per % below floor

# Normalization target (sum of weights); use 1.0 for ratio, 100.0 for percent
NORMALIZE_TO       = float(os.getenv("REBUY_NORMALIZE_TO", "100.0"))

# Minimal weight cutoff (after normalization); anything smaller is written as blank to reduce noise
MIN_OUTPUT_CUTOFF  = float(os.getenv("REBUY_MIN_OUTPUT_CUTOFF", "0.0"))

# Rounding for output presentation
ROUND_DECIMALS     = int(os.getenv("REBUY_ROUND_DECIMALS", "2"))

# ============================================================================

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def _hmap(header):
    return {str_or_empty(h): i for i, h in enumerate(header)}

def _has_any_token(tag_str: str, tokens_upper):
    t = str_or_empty(tag_str).upper()
    return any(tok and tok in t for tok in tokens_upper)

def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else (1.0 if x > 1.0 else x)

@with_sheet_backoff
def run_rebuy_weight_calculator():
    print("🧠 Rebuy Weights…")
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    vals = get_values_cached(STATS_TAB, ttl_s=TTL_S) or []
    if not vals or not vals[0]:
        print(f"ℹ️ {STATS_TAB} empty; skipping.")
        return

    header = vals[0]
    h = _hmap(header)
    i_tok = h.get(COL_TOKEN)

    # Optional columns
    i_mem = h.get(COL_MEMORY_SCORE) if COL_MEMORY_SCORE in h else h.get("Memory Score")
    i_perf = h.get(COL_PERFORMANCE) if COL_PERFORMANCE in h else None
    i_days = h.get(COL_DAYS_HELD)   if COL_DAYS_HELD   in h else None
    i_stat = h.get(COL_STATUS)      if COL_STATUS      in h else None
    i_tags = h.get(COL_TAGS)        if COL_TAGS        in h else None

    missing = []
    if i_tok is None: missing.append(COL_TOKEN)
    if i_mem is None: missing.append(COL_MEMORY_SCORE)
    if missing:
        print(f"⚠️ Missing columns on {STATS_TAB}: {', '.join(missing)}; cannot compute weights.")
        return

    # Ensure output column exists
    header_writes = []
    if COL_OUTPUT in h:
        i_out = h[COL_OUTPUT]
    else:
        header.append(COL_OUTPUT)
        i_out = len(header) - 1
        header_writes.append({"range": f"{_col_letter(i_out+1)}1", "values": [[COL_OUTPUT]]})

    # 1) Build raw scores per row
    rows_info = []
    for r_idx, row in enumerate(vals[1:], start=2):
        token = str_or_empty(row[i_tok] if i_tok < len(row) else "").upper()
        if not token:
            continue

        # Optional status filter
        status_ok = True
        if i_stat is not None and i_stat < len(row) and ACTIVE_ONLY:
            status_ok = str_or_empty(row[i_stat]).upper() == ACTIVE_VALUE
        if not status_ok:
            rows_info.append((r_idx, token, 0.0, ""))  # 0 score; still allow clearing
            continue

        mem = to_float(row[i_mem] if i_mem < len(row) else "", default=0.0)
        mem = max(mem, 0.0)

        # soft days decay
        if i_days is not None and i_days < len(row):
            try:
                d = float(str_or_empty(row[i_days]).split(".")[0] or "0")
            except:  # noqa: E722
                d = 0.0
            if d > DAYS_DECAY_START:
                decay_steps = d - DAYS_DECAY_START
                mem *= (1.0 - _clamp01(DAYS_DECAY_RATE)) ** decay_steps

        # soft perf decay if under floor
        if i_perf is not None and i_perf < len(row):
            perf = to_float(row[i_perf], default=None)
            if perf is not None and perf < PERF_SOFT_FLOOR:
                below = PERF_SOFT_FLOOR - perf  # positive deficit
                mem *= (1.0 - _clamp01(PERF_DECAY_RATE)) ** below

        # tag boost
        if i_tags is not None and i_tags < len(row):
            tags = str_or_empty(row[i_tags])
            if _has_any_token(tags, TOKENS_BOOST):
                mem *= max(BOOST_MULT, 1.0)

        rows_info.append((r_idx, token, mem, str_or_empty(row[i_out] if i_out < len(row) else "")))

    # 2) Normalize to NORMALIZE_TO
    total = sum(score for _, _, score, _ in rows_info)
    if total <= 0:
        # If all zeros, clear outputs to avoid stale weights
        writes = []
        for r_idx, _, _, cur_out in rows_info:
            if cur_out != "":
                writes.append({"range": f"{_col_letter(i_out+1)}{r_idx}", "values": [[""]]})
        if header_writes or writes:
            ws = get_ws(STATS_TAB)
            ws_batch_update(ws, [*header_writes, *writes])
            print(f"✅ Rebuy Weights: cleared {len(writes)} cell(s){' + header' if header_writes else ''}.")
        else:
            print("ℹ️ Rebuy Weights: nothing to write (all zeros).")
        return

    # 3) Prepare diffs (write only changes)
    writes, touched = [], 0
    for r_idx, token, score, cur_out in rows_info:
        w = NORMALIZE_TO * (score / total)
        if w < MIN_OUTPUT_CUTOFF:
            new_s = ""
        else:
            new_s = f"{round(w, ROUND_DECIMALS)}"

        if (cur_out or "") != (new_s or ""):
            writes.append({"range": f"{_col_letter(i_out+1)}{r_idx}", "values": [[new_s]]})
            touched += 1
            if touched >= MAX_WRITES:
                break

    if not header_writes and not writes:
        print("✅ Rebuy Weights: no changes needed.")
        return

    ws = get_ws(STATS_TAB)
    payload = []
    if header_writes: payload.extend(header_writes)
    if writes:        payload.extend(writes)
    ws_batch_update(ws, payload)

    print(f"✅ Rebuy Weights: wrote {touched} cell(s){' + header' if header_writes else ''}.")
