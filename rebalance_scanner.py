# rebalance_scanner.py
from utils import get_ws, ws_batch_update, to_float, send_telegram_message_dedup, with_sheet_backoff

@with_sheet_backoff
def run_rebalance_scanner():
    ws = get_ws("Portfolio_Targets")
    rows = ws.get_all_records()

    header = ws.row_values(1)
    col_idx = {name: i + 1 for i, name in enumerate(header)}

    drift_col = col_idx.get("Drift")
    if not drift_col:
        header.append("Drift")
        ws.update("A1", [header])
        drift_col = len(header)

    updates = []
    for r_i, rec in enumerate(rows, start=2):
        cur = to_float(rec.get("Current %"))
        tgt = to_float(rec.get("Target %"))
        if cur is None or tgt is None:
            continue
        drift = round(cur - tgt, 2)
        col_letter = chr(64 + drift_col)
        updates.append({"range": f"Portfolio_Targets!{col_letter}{r_i}", "values": [[drift]]})

    if updates:
        ws_batch_update(ws, updates)
        send_telegram_message_dedup(
            f"🔎 Rebalance scan updated {len(updates)} drift cell(s).",
            key="rebalance_scan",
            ttl_min=60
        )
