# claim_tracker.py
import datetime as _dt

from utils import (
    get_ws,
    safe_get_all_records,
    ws_batch_update,
    with_sheet_backoff,
    str_or_empty,
)

SHEET_NAME = "Claim_Tracker"

def _col_letter(idx_1b: int) -> str:
    n = idx_1b
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _parse_dt(s: str):
    s = str_or_empty(s)
    if not s:
        return None
    # try common formats; keep lightweight
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m/%d/%Y %H:%M"):
        try:
            return _dt.datetime.strptime(s, fmt)
        except Exception:
            pass
    # last resort: isoformat-ish
    try:
        return _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

@with_sheet_backoff
def run_claim_tracker():
    """
    Updates 'Days Since Unlock' (if that column exists) based on 'Arrival Date'.
    Does a single batch update to minimize writes.
    Gracefully no-ops if headers are missing.
    """
    ws = get_ws(SHEET_NAME)

    header = ws.row_values(1) or []
    idx = {h: i + 1 for i, h in enumerate(header)}  # 1-based
    arrival_col = idx.get("Arrival Date")
    days_col = idx.get("Days Since Unlock") or idx.get("Days Since") or idx.get("Days")

    if arrival_col is None:
        print("ℹ️ Claim Tracker: no 'Arrival Date' column; skipping.")
        return

    # If Days column is missing, create 'Days Since Unlock'
    if days_col is None:
        header.append("Days Since Unlock")
        ws.update("A1", [header])  # atomic header write
        days_col = len(header)

    rows = safe_get_all_records(ws, ttl_s=120)
    now = _dt.datetime.utcnow()
    updates = []

    for r_idx, rec in enumerate(rows, start=2):
        arrival_raw = rec.get("Arrival Date", "")
        dt = _parse_dt(arrival_raw)
        a1 = f"{SHEET_NAME}!{_col_letter(days_col)}{r_idx}"

        if not dt:
            # clear the days cell if unparsable/empty
            updates.append({"range": a1, "values": [[""]]})
            continue

        # If parsed as timezone-aware, convert to naive UTC for subtraction
        if dt.tzinfo is not None:
            dt = dt.astimezone(_dt.timezone.utc).replace(tzinfo=None)

        days = max(0, (now - dt).days)
        updates.append({"range": a1, "values": [[str(days)]]})

    if updates:
        ws_batch_update(ws, updates)
        print(f"✅ Claim tracker complete: {len(updates)} cell(s) updated.")
    else:
        print("ℹ️ Claim tracker: nothing to update.")
