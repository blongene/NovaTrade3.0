# rebalance_scanner.py
from utils import get_ws, ws_batch_update, to_float, with_sheet_backoff, send_telegram_message_dedup

def _letter(col_idx_1_based: int) -> str:
    # simple A..Z (your sheet columns here are within that range)
    return chr(64 + col_idx_1_based)

@with_sheet_backoff
def run_rebalance_scan():
    """
    Compute Drift = Current % - Target % for each row in Portfolio_Targets
    and write all changes in a single batch update. Sends a de-duped
    Telegram summary once per hour.
    """
    ws = get_ws("Portfolio_Targets")
    rows = ws.get_all_records()

    # Resolve/ensure headers
    header = ws.row_values(1) or []
    idx = {name: i + 1 for i, name in enumerate(header)}  # 1-based
    drift_col = idx.get("Drift")
    if not drift_col:
        header.append("Drift")
        ws.update("A1", [header])  # atomic header write
        drift_col = len(header)
        idx["Drift"] = drift_col

    updates = []
    for r_i, rec in enumerate(rows, start=2):  # data starts at row 2
        cur = to_float(rec.get("Current %"))
        tgt = to_float(rec.get("Target %"))
        if cur is None or tgt is None:
            continue
        drift = round(cur - tgt, 2)
        updates.append({
            "range": f"Portfolio_Targets!{_letter(drift_col)}{r_i}",
            "values": [[drift]]
        })

    if updates:
        ws_batch_update(ws, updates)
        send_telegram_message_dedup(
            f"🔎 Rebalance scan updated {len(updates)} drift cell(s).",
            key="rebalance_scan",
            ttl_min=60
        )

# Back-compat: some code imports run_rebalance_scanner
def run_rebalance_scanner():
    return run_rebalance_scan()
