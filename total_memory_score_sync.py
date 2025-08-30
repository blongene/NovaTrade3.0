# total_memory_score_sync.py — NT3.0 Render Phase-1 polish
# Computes "Total Memory Score" from one or more component columns in Rotation_Stats
# using a single cached read and one batched write (with jitter & backoff).

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff, str_or_empty
)

TAB = os.getenv("MEM_TOTAL_TAB", "Rotation_Stats")
DEST_COL_NAME = os.getenv("MEM_TOTAL_DEST_COL", "Total Memory Score")

# Comma-separated list of component columns to sum/weight
# e.g.: "Memory Score,Vault Memory Score,Rebuy Memory Score"
COMPONENT_COLS = [c.strip() for c in os.getenv(
    "MEM_TOTAL_COMPONENT_COLS",
    "Memory Score,Vault Memory Score,Rebuy Memory Score"
).split(",") if c.strip()]

# Optional weights aligned to COMPONENT_COLS, e.g. "1,1,0.5"
_WEIGHTS_ENV = [w.strip() for w in os.getenv("MEM_TOTAL_WEIGHTS", "").split(",") if w.strip()]
if _WEIGHTS_ENV and len(_WEIGHTS_ENV) != len(COMPONENT_COLS):
    # length mismatch → ignore weights to avoid confusion
    _WEIGHTS_ENV = []
WEIGHTS = []
for w in _WEIGHTS_ENV:
    try:
        WEIGHTS.append(float(w))
    except Exception:
        WEIGHTS.append(1.0)
if not WEIGHTS:
    WEIGHTS = [1.0] * len(COMPONENT_COLS)

TTL_READ_S   = int(os.getenv("MEM_TOTAL_TTL_SEC", "300"))     # cache reads 5m
MIN_DIFF     = float(os.getenv("MEM_TOTAL_MIN_DIFF", "0.01")) # 0.01 points threshold
MAX_WRITES   = int(os.getenv("MEM_TOTAL_MAX_WRITES", "500"))
JITTER_MIN_S = float(os.getenv("MEM_TOTAL_JITTER_MIN_S", "0.3"))
JITTER_MAX_S = float(os.getenv("MEM_TOTAL_JITTER_MAX_S", "1.2"))

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

def _weighted_total(values):
    total, wsum = 0.0, 0.0
    for val, w in values:
        if val is None:
            continue
        total += val * w
        wsum  += w
    if wsum == 0:
        return None
    return total  # if you prefer normalized: return total / wsum

@with_sheet_backoff
def sync_total_memory_score():
    print("▶️ Total memory score sync …")
    time.sleep(random.uniform(JITTER_MIN_S, JITTER_MAX_S))  # de-sync neighbors

    vals = get_values_cached(TAB, ttl_s=TTL_READ_S) or []
    if not vals:
        print(f"ℹ️ {TAB} empty; skipping.")
        return

    header = vals[0]
    hidx = {h: i for i, h in enumerate(header)}  # 0-based

    # Check components exist (some may be missing; we’ll use the ones that exist)
    comp_idxs = []
    for name in COMPONENT_COLS:
        if name in hidx:
            comp_idxs.append(hidx[name])
        else:
            print(f"ℹ️ Skipping missing component column: {name}")

    if not comp_idxs:
        print(f"⚠️ No component columns found in {TAB}; nothing to compute.")
        return

    # Ensure destination column; if missing, add header at the end.
    dest_idx = hidx.get(DEST_COL_NAME)
    writes_header = []
    if dest_idx is None:
        dest_idx = len(header)  # next new column (0-based)
        a1 = f"{_col_letter(dest_idx + 1)}1"
        writes_header.append({"range": a1, "values": [[DEST_COL_NAME]]})

    # Stage row updates
    writes_rows = []
    touched = 0

    for r_idx, row in enumerate(vals[1:], start=2):  # data starts at row 2
        token = str_or_empty(row[hidx.get("Token", -1)] if hidx.get("Token", -1) < len(row) else "")
        if not token:
            continue  # skip blank lines

        # Collect component values (float or None)
        comps = []
        for j, w in zip(comp_idxs, WEIGHTS):
            val = _to_float(row[j] if j < len(row) else None)
            comps.append((val, w))

        tot = _weighted_total(comps)
        if tot is None:
            continue

        # Current destination value
        cur = _to_float(row[dest_idx] if dest_idx < len(row) else None)

        if cur is not None and abs(cur - tot) < MIN_DIFF:
            continue

        cell = f"{_col_letter(dest_idx + 1)}{r_idx}"
        writes_rows.append({"range": cell, "values": [[f"{tot}"]]})
        touched += 1
        if touched >= MAX_WRITES:
            break

    if not writes_header and not writes_rows:
        print("✅ Total memory score: no changes needed.")
        return

    ws = get_ws(TAB)  # open only if we’re writing
    payload = []
    if writes_header:
        payload.extend(writes_header)
    if writes_rows:
        payload.extend(writes_rows)
    ws_batch_update(ws, payload)
    print(f"✅ Total memory score: wrote {len(writes_rows)} cell(s){' + header' if writes_header else ''}.")
