# total_memory_score_sync.py

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import (
    with_sheet_backoff,
    get_ws,
    get_values_cached,
    ws_batch_update,
    safe_float,               # already strips % and returns 0.0 on blanks
)

SHEET_NAME = "Rotation_Stats"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _header_map(header_row):
    # 0-based indices for list access; +1 later when building A1
    return {str(h).strip(): i for i, h in enumerate(header_row)}

def _ensure_header(header_row, name):
    """Return (updated_header_row, index_of_name)."""
    if name in header_row:
        return header_row, header_row.index(name)
    header_row = list(header_row) + [name]
    return header_row, len(header_row) - 1

def _num_or_zero(v):
    # Use safe_float (handles %, blanks) but keep a tiny guard
    try:
        return float(safe_float(v, 0.0))
    except Exception:
        return 0.0

@with_sheet_backoff
def _write_header(ws, header_row):
    # Write the entire header row in one call
    ws.update("A1", [header_row])

def sync_total_memory_score():
    print("▶️ Total memory score sync …")
    try:
        ws = get_ws(SHEET_NAME)
        vals = get_values_cached(SHEET_NAME, ttl_s=15)  # [[...], [...], ...]
        if not vals:
            print("⚠️ Rotation_Stats is empty.")
            return

        header = list(vals[0])
        hmap = _header_map(header)

        # Columns we sum (present or not). Add more here if needed later.
        COMPONENT_COLS = ["Memory Weight", "Memory Vault Score", "Rebuy Weight"]

        # Make sure Total column exists
        header, total_ix = _ensure_header(header, "Total Memory Score")
        if header != vals[0]:
            _write_header(ws, header)
            # refresh header map after write
            hmap = _header_map(header)

        # Prepare updates
        updates = []
        total_col_a1 = _col_letter(total_ix + 1)  # A1 is 1-based
        rows = vals[1:]

        # Locate component indices (if a component header is missing, treat as 0)
        comp_ix = {name: hmap.get(name, None) for name in COMPONENT_COLS}

        for r_i, row in enumerate(rows, start=2):
            # Optional: only compute if a Token exists (reduces noise)
            tok_ix = hmap.get("Token")
            token = (row[tok_ix].strip().upper() if tok_ix is not None and tok_ix < len(row) else "")
            if not token:
                # nothing to score
                continue

            # Sum the components; blanks -> 0
            total = 0.0
            for name, ix in comp_ix.items():
                val = row[ix] if (ix is not None and ix < len(row)) else ""
                total += _num_or_zero(val)

            a1 = f"{total_col_a1}{r_i}"
            updates.append({"range": a1, "values": [[total]]})

        if updates:
            ws_batch_update(ws, updates)
        print("✅ Total Memory Score sync complete.")
    except Exception as e:
        print(f"❌ Error in sync_total_memory_score: {e}")
