# target_percent_updater.py

import re
from utils import (
    get_ws,
    get_values_cached,
    ws_batch_update,
    with_sheet_backoff,
    safe_float,
)

SHEET_NAME = "Portfolio_Targets"

def _col_letter(n: int) -> str:
    """1-based column number -> A1 letters."""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_target_percent_updater():
    print("▶️ Target % updater …")

    # One cached read of the entire sheet
    vals = get_values_cached(SHEET_NAME, ttl_s=120) or []
    if not vals:
        print("⚠️ Empty Portfolio_Targets sheet.")
        return

    header = vals[0]

    def _col_ix(name: str):
        try:
            return header.index(name) + 1  # A1 is 1-based
        except ValueError:
            return None

    token_col     = _col_ix("Token")
    suggested_col = _col_ix("Suggested %")
    target_col    = _col_ix("Target %")

    missing = [n for n, c in [("Token", token_col),
                              ("Suggested %", suggested_col),
                              ("Target %", target_col)] if c is None]
    if missing:
        print(f"⚠️ {SHEET_NAME} missing columns: {', '.join(missing)}")
        return

    updates = []
    # Walk rows once; build batched single-cell updates
    for r_idx, row in enumerate(vals[1:], start=2):
        token = (row[token_col - 1] if len(row) >= token_col else "").strip().upper()
        if not token:
            continue

        suggested_raw = row[suggested_col - 1] if len(row) >= suggested_col else ""
        target_raw    = row[target_col - 1] if len(row) >= target_col else ""

        # Only act if Suggested % is a number and Target % is empty
        s_val = safe_float(suggested_raw, None)
        t_val = (target_raw or "").strip()

        if s_val is None or t_val:
            continue

        a1 = f"{_col_letter(target_col)}{r_idx}"
        # write e.g. "12.34%" (Sheets will keep it as text unless the column is formatted as percent)
        updates.append({"range": a1, "values": [[f"{s_val}%"]]})

    if not updates:
        print("✅ Target % update complete. 0 tokens adjusted.")
        return

    # Single batched write
    ws = get_ws(SHEET_NAME)
    ws_batch_update(ws, updates)
    print(f"✅ Target % update complete. {len(updates)} token(s) adjusted.")
