# target_percent_updater.py — NovaTrade 3.0 (Phase-1 Polish)
# Fills Target % if blank, using Suggested %
# - Cached read
# - Batch write
# - Safe float parsing

from utils import (
    get_ws,
    get_values_cached,
    ws_batch_update,
    str_or_empty,
    to_float,
    with_sheet_backoff,
)

SHEET_NAME = "Portfolio_Targets"

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_target_percent_updater():
    print("▶️ Target % updater …")

    vals = get_values_cached(SHEET_NAME, ttl_s=180) or []
    if not vals:
        print("⚠️ Empty Portfolio_Targets sheet.")
        return

    header = [str_or_empty(h) for h in vals[0]]

    def _col_ix(name: str):
        try:
            return header.index(name) + 1
        except ValueError:
            return None

    token_col     = _col_ix("Token")
    suggested_col = _col_ix("Suggested %")
    target_col    = _col_ix("Target %")

    if not all([token_col, suggested_col, target_col]):
        print("⚠️ Missing one of required columns: Token, Suggested %, Target %")
        return

    updates = []
    for r_idx, row in enumerate(vals[1:], start=2):
        token = str_or_empty(row[token_col-1]).upper()
        if not token:
            continue

        suggested_val = to_float(row[suggested_col-1], None) if len(row) >= suggested_col else None
        target_val    = str_or_empty(row[target_col-1]) if len(row) >= target_col else ""

        if suggested_val is not None and not target_val:
            a1 = f"{_col_letter(target_col)}{r_idx}"
            updates.append({"range": a1, "values": [[f"{suggested_val}%"]]})
            print(f"➕ {token} → {suggested_val}%")

    if updates:
        ws = get_ws(SHEET_NAME)
        ws_batch_update(ws, updates)
        print(f"✅ Target % update complete. {len(updates)} token(s) adjusted.")
    else:
        print("✅ Target % update complete. 0 changes.")
