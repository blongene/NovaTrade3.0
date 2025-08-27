# rebalance_scanner.py
import os
from typing import List, Dict, Tuple
from utils import (
    get_ws,
    get_records_cached,
    with_sheet_backoff,
    safe_float,
    send_telegram_message_dedup,
)

# Which column name should receive the drift status text
_STATUS_COL_NAME = os.getenv("REBALANCE_STATUS_COL", "Status")
# Telegram de-dupe key & TTL (minutes)
_TG_KEY = "rebalance_alert"
_TG_TTL_MIN = int(os.getenv("REBALANCE_ALERT_TTL_MIN", "60"))

def _col_letter(c: int) -> str:
    s = ""
    while c:
        c, r = divmod(c - 1, 26)
        s = chr(65 + r) + s
    return s

def _find_col_index(header: List[str], name: str) -> int:
    # case-insensitive match; returns 1-based column index or -1 if not found
    low = [h.strip().lower() for h in header]
    try:
        return low.index(name.strip().lower()) + 1
    except ValueError:
        return -1

def _format_pct(x: float) -> str:
    # keep whatever precision the sheet uses visually (no percent sign here)
    return f"{x:.2f}"

def _drift_row(row: Dict[str, str]) -> Tuple[str, str]:
    """
    Returns (token, drift_status)
      - "Undersized" if Current % < Min %
      - "Overweight" if Current % > Max %
      - "On target" otherwise
    """
    token = (row.get("Token") or "").strip()
    current = safe_float(row.get("Current %"))
    min_pct = safe_float(row.get("Min %"), 0.0)
    max_pct = safe_float(row.get("Max %"), 100.0)

    if token == "":
        return "", ""
    if current < min_pct:
        return token, "Undersized"
    if current > max_pct:
        return token, "Overweight"
    return token, "On target"

@with_sheet_backoff
def _batch_write_status(ws, updates: List[Tuple[int, int, str]]):
    """Batch update status cells. updates = [(row_index, col_index, value), ...]"""
    if not updates:
        return
    body = []
    for r, c, v in updates:
        body.append({"range": f"{_col_letter(c)}{r}", "values": [[v]]})
    # Single call
    ws.batch_update(body, value_input_option="USER_ENTERED")

def _build_alert_lines(drift_items: List[Tuple[str, float, float]]) -> str:
    # items: (token, current, target)
    lines = []
    for token, current, target in drift_items:
        lines.append(f"{token}: {current:.2f}% (Target: {target:.2f}%)")
    return "\n".join(lines)

def _collect_drift_items(rows: List[Dict[str, str]]) -> List[Tuple[str, float, float]]:
    items = []
    for r in rows:
        token = (r.get("Token") or "").strip()
        if not token:
            continue
        current = safe_float(r.get("Current %"))
        target = safe_float(r.get("Target %"))
        min_pct = safe_float(r.get("Min %"), 0.0)
        max_pct = safe_float(r.get("Max %"), 100.0)
        if current < min_pct or current > max_pct:
            items.append((token, current, target))
    return items

@with_sheet_backoff
def _set_nova_trigger():
    try:
        ws = get_ws("NovaTrigger")
        ws.update_acell("A1", "REBALANCE ALERT")
    except Exception:
        # soft-fail; not critical
        pass

def run_rebalance_scanner():
    try:
        ws = get_ws("Portfolio_Targets")
        # Cached read to avoid hammering Sheets
        rows = get_records_cached("Portfolio_Targets", ttl_s=90)
        if not rows:
            print("ℹ️ Rebalance: no rows.")
            return

        # header fetch is a single cheap call
        header = ws.row_values(1)
        status_col = _find_col_index(header, _STATUS_COL_NAME)
        if status_col == -1:
            # fallback to legacy column H if present
            status_col = 8  # H
        updates = []
        drift_alerts = []

        for i, r in enumerate(rows, start=2):  # data starts at row 2
            token, status = _drift_row(r)
            if not token:
                continue

            # minimize writes: only update if different
            existing = (r.get(_STATUS_COL_NAME) or "").strip()
            if existing != status:
                updates.append((i, status_col, status))

        # one batched write
        _batch_write_status(ws, updates)

        # build alert set from cached rows (no extra reads)
        drift_items = _collect_drift_items(rows)
        if drift_items:
            lines = _build_alert_lines(drift_items)
            msg = (
                "📊 <b>Portfolio Drift Detected</b>\n\n"
                + lines.replace("\n", "\n• ")
                + "\n\nReply YES to rebalance or SKIP to ignore."
            )
            # de-dupe alerts so you get at most one per hour unless content changes
            send_telegram_message_dedup(msg, key=_TG_KEY, ttl_min=_TG_TTL_MIN)
            _set_nova_trigger()

        # quiet success
    except Exception as e:
        print(f"❌ Rebalance scanner error: {e}")
