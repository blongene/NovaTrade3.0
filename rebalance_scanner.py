import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, send_telegram_message_dedup

DEBUG = os.getenv("DEBUG", "0") == "1"
def _log(msg: str):
    if DEBUG:
        print(msg)

@with_sheet_backoff
def _ws_get_all_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _ws_update_acell(ws, a1, v):
    return ws.update_acell(a1, v)

@with_sheet_backoff
def _ws_batch_update(ws, updates):
    """
    updates: list of (a1, value) to write; we coalesce into a single request.
    """
    data = [{"range": a1, "values": [[value]]} for a1, value in updates]
    return ws.spreadsheet.values_batch_update(
        ws.spreadsheet.id,
        body={"valueInputOption": "USER_ENTERED", "data": data}
    )

def _to_float(v, default=0.0):
    try:
        return float(str(v).strip().replace("%", ""))
    except Exception:
        return default

def run_rebalance_scanner():
    """
    Scans Portfolio_Targets for drift and updates 'Drift Status' (col H).
    Sends ONE de‑duplicated Telegram alert if any drift is found.
    Quiet logs unless DEBUG=1.
    """
    _log("rebalance_scanner: start")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))
    ws = sheet.worksheet("Portfolio_Targets")

    rows = _ws_get_all_records(ws)

    drift_alerts = []
    batch_updates = []  # list of (a1, value)

    # Determine column H row A1 target for each record (header is on row 1)
    for idx, row in enumerate(rows, start=2):
        token = (row.get("Token", "") or "").strip()
        target = _to_float(row.get("Target %", 0))
        min_pct = _to_float(row.get("Min %", 0))
        max_pct = _to_float(row.get("Max %", 100))
        current = _to_float(row.get("Current %", 0))

        status = "On target"
        if current < min_pct:
            status = "Undersized"
            drift_alerts.append(f"🔽 {token}: {current}% (Target {target}%)")
        elif current > max_pct:
            status = "Overweight"
            drift_alerts.append(f"🔼 {token}: {current}% (Target {target}%)")

        a1 = f"H{idx}"  # Drift Status column
        batch_updates.append((a1, status))

    # batch write drift statuses (single API call)
    if batch_updates:
        try:
            _ws_batch_update(ws, batch_updates)
        except AttributeError:
            # Fallback for older gspread; do per‑cell with backoff as last resort
            for a1, v in batch_updates:
                _ws_update_acell(ws, a1, v)

    if drift_alerts:
        text = "📊 <b>Portfolio Drift Detected</b>\n\n" + "\n".join(drift_alerts) + "\n\nReply YES to rebalance or SKIP to ignore."
        # de‑dupe key keeps spam away if the scanner runs frequently
        send_telegram_message_dedup(text, key="rebalance_alerts", ttl_min=30)
        _log("rebalance_scanner: alert sent")
    else:
        _log("rebalance_scanner: no drift")

    _log("rebalance_scanner: done")
