# top_token_summary.py
import re
from datetime import datetime
from utils import (
    get_ws,
    safe_get_all_records,
    ws_batch_update,
    with_sheet_backoff,
)

def _to_float(s):
    try:
        return float(str(s).replace("%", "").strip())
    except Exception:
        return None

@with_sheet_backoff
def run_top_token_summary():
    print("📈 Running Top Token ROI Summary...")

    stats_ws = get_ws("Rotation_Stats")

    # one cached read pass
    stats_rows = safe_get_all_records(stats_ws, ttl_s=180)
    if not stats_rows:
        print("ℹ️ Rotation_Stats empty; nothing to summarize.")
        return

    # Ensure headers and find columns (robust to missing)
    headers = stats_ws.row_values(1)
    if not headers:
        print("ℹ️ Rotation_Stats has no header.")
        return

    # Find (or add) columns for: Milestone/Last Alerted if you use it
    try:
        token_col_idx = headers.index("Token") + 1
    except ValueError:
        print("ℹ️ No 'Token' column in Rotation_Stats; skipping.")
        return

    if "Follow-up ROI" in headers:
        followup_col_idx = headers.index("Follow-up ROI") + 1
    elif "ROI %" in headers:
        followup_col_idx = headers.index("ROI %") + 1
    elif "ROI" in headers:
        followup_col_idx = headers.index("ROI") + 1
    else:
        print("ℹ️ No 'Follow-up ROI'/'ROI %'/'ROI' in Rotation_Stats; skipping.")
        return

    # Choose top winners by Follow-up ROI (numeric)
    enriched = []
    for i, rec in enumerate(stats_rows, start=2):  # data rows start at 2
        token = (rec.get("Token") or "").strip().upper()
        if not token:
            continue
        roi_raw = rec.get("Follow-up ROI") or rec.get("ROI %") or rec.get("ROI")
        roi = _to_float(roi_raw)
        if roi is None:
            continue
        enriched.append((i, token, roi))

    if not enriched:
        print("ℹ️ No numeric ROI in Rotation_Stats; nothing to alert.")
        return

    # simple example: pick top 3 > some threshold (e.g., >100%)
    winners = sorted(enriched, key=lambda x: x[2], reverse=True)[:3]

    # If you need to write a 'Milestone' column, batch once:
    # Ensure the column exists
    if "Last Alerted" in headers:
        last_alert_col_idx = headers.index("Last Alerted") + 1
    else:
        headers.append("Last Alerted")
        # Write header + no row shift
        ws_batch_update(stats_ws, [{"range": "A1", "values": [headers]}])
        last_alert_col_idx = len(headers)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    updates = []
    alerts_sent = 0

    for row_idx, token, roi in winners:
        # Example policy: only alert when ROI >= 200
        if roi >= 200:
            a1 = f"{_col_letter(last_alert_col_idx)}{row_idx}"
            updates.append({"range": f"Rotation_Stats!{a1}", "values": [[f"{roi:.2f}% at {now}"]]})
            alerts_sent += 1

    if updates:
        ws_batch_update(stats_ws, updates)

    print(f"✅ Top Token Summary complete. {alerts_sent} alert(s) sent.")

def _col_letter(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s
