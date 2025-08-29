# rebuy_memory_engine.py

import os
from datetime import datetime

# Use the guarded/cached helpers you already have
from utils import (
    with_sheet_backoff,
    get_ws,
    get_records_cached,   # cached by sheet name
    ws_batch_update,      # gated + backoff + batch
    safe_float,
)

SHEET_STATS = "Rotation_Stats"

def _col_letter(n: int) -> str:
    """1-based column index -> Excel letters."""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def _open_stats_ws():
    return get_ws(SHEET_STATS)

def run_memory_rebuy_scan():
    print("▶️ Memory rebuy scan …")
    print("🔁 Running Memory-Aware Rebuy Scan...")

    try:
        ws = _open_stats_ws()

        # One cached read of all records (dicts). This hits the in-process cache if fresh.
        rows = get_records_cached(SHEET_STATS, ttl_s=180)
        if not rows:
            print("ℹ️ Rotation_Stats is empty; nothing to do.")
            return

        # Get header (names) and build index map once
        header = list(rows[0].keys())
        hidx = {name: (i + 1) for i, name in enumerate(header)}  # 1-based

        # Required columns
        token_col        = hidx.get("Token")
        memory_score_col = hidx.get("Memory Score") or hidx.get("MemoryScore")
        rebuy_weight_col = hidx.get("Rebuy Weight") or hidx.get("RebuyWeight")

        missing = []
        if not token_col:        missing.append("Token")
        if not memory_score_col: missing.append("Memory Score")
        if not rebuy_weight_col: missing.append("Rebuy Weight")

        # If any required col is missing, exit quietly (no sheet churn)
        if missing:
            print(f"ℹ️ Memory rebuy engine: missing columns: {', '.join(missing)}. Skipping.")
            return

        # Prepare batched updates
        updates = []
        changed = 0

        # Example formula:
        # Rebuy Weight = clamp( Memory Score, 0 .. 5 )
        # (You can adjust to your actual scoring model later.)
        for r_idx, rec in enumerate(rows, start=2):  # 2 = first data row
            token = str(rec.get("Token", "")).strip().upper()
            if not token:
                continue

            ms_raw = rec.get("Memory Score", rec.get("MemoryScore", ""))
            current_rw_raw = rec.get("Rebuy Weight", rec.get("RebuyWeight", ""))

            ms = safe_float(ms_raw, 0.0)
            # Simple clamp
            target_rw = max(0.0, min(ms, 5.0))

            # If sheet already matches, skip writing
            current_rw = safe_float(current_rw_raw, None)
            if current_rw is not None and abs(current_rw - target_rw) < 1e-9:
                continue

            # Prepare single-cell update (no sheet prefix, just A1)
            a1 = f"{_col_letter(rebuy_weight_col)}{r_idx}"
            updates.append({"range": a1, "values": [[target_rw]]})
            changed += 1

        # Batch write once if needed
        if updates:
            ws_batch_update(ws, updates)
            print(f"✅ Rebuy Weights updated for {changed} token(s).")
        else:
            print("ℹ️ Rebuy Weights already up to date.")

        print("✅ Rebuy memory scan complete.")

    except Exception as e:
        # Let your global logs show the precise error
        print(f"❌ Error in run_rebuy_memory_engine: {e}")
