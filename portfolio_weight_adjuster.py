# portfolio_weight_adjuster.py — NT3.0 Phase-1 Polish
# Copies "Suggested %" → "Target %" in Rotation_Stats using values-only reads + one batch write.

import os, time, random
from utils import (
    get_values_cached, get_ws, ws_batch_update,
    with_sheet_backoff, str_or_empty
)

TAB = "Rotation_Stats"

TTL_READ_S   = int(os.getenv("PWA_TTL_READ_SEC", "300"))   # cache reads 5m
MAX_WRITES   = int(os.getenv("PWA_MAX_WRITES", "400"))     # cap per run
JITTER_MIN_S = float(os.getenv("PWA_JITTER_MIN_S", "0.5"))
JITTER_MAX_S = float(os.getenv("PWA_JITTER_MAX_S", "2.0"))

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65 + r) + s
    return s

def _to_float(v, default=None):
    try:
        s = str(v).replace("%", "").replace(",", "").strip()
        return float(s) if s else default
    except Exception:
        return default

@with_sheet_backoff
def run_portfolio_weight_adjuster():
    print("▶ Portfolio weight adjuster …")
    time.sleep(random.uniform(JITTER_MIN_S, JITTER_MAX_S))  # de-sync from neighbors

    vals = get_values_cached(TAB, ttl_s=TTL_READ_S) or []
    if not vals:
        print("ℹ️ Rotation_Stats empty; skipping.")
        return

    header = vals[0]
    col = {h: i+1 for i, h in enumerate(header)}

    # Ensure required columns exist
    need = ["Token", "Suggested %", "Target %"]
    missing = [h for h in need if h not in col]
    if missing:
        print(f"⚠️ Missing columns in {TAB}: {', '.join(missing)}; skipping.")
        return

    tok_c = col["Token"] - 1
    sug_c = col["Suggested %"] - 1
    tgt_c = col["Target %"] - 1

    writes = []
    touched = 0

    # Build per-row updates, packing contiguous ranges for efficiency
    for r_idx, row in enumerate(vals[1:], start=2):  # data starts at row 2
        token = str_or_empty(row[tok_c] if tok_c < len(row) else "").upper()
        if not token:
            continue

        sug = _to_float(row[sug_c] if sug_c < len(row) else None)
        tgt = _to_float(row[tgt_c] if tgt_c < len(row) else None)

        # Only write if Suggested % is present and differs from Target % by >= 0.01pp
        if sug is None:
            continue
        if tgt is not None and abs(sug - tgt) < 0.01:
            continue

        # Prepare one-row contiguous write (just the Target % cell)
        cell = _col_letter(tgt_c + 1) + str(r_idx)
        writes.append({"range": f"{cell}", "values": [[f"{sug}"]]})
        touched += 1
        if touched >= MAX_WRITES:
            break

    if not writes:
        print("✅ Portfolio weight adjuster: no changes needed.")
        return

    ws = get_ws(TAB)  # open only when we actually write
    ws_batch_update(ws, writes)
    print(f"✅ Portfolio weight adjuster: updated {touched} Target % cell(s).")
