import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, send_telegram_message

def _gclient():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds)

@with_sheet_backoff
def _open_ws(sheet_url: str, title: str):
    sh = _gclient().open_by_url(sheet_url)
    return sh.worksheet(title)

def _to_float(v, default=0.0):
    try:
        return float(str(v).strip().replace("%", ""))
    except Exception:
        return default

def run_rebalance_scanner():
    print("🔁 Running Rebalance Scanner...")
    sheet_url = os.getenv("SHEET_URL")
    ws = _open_ws(sheet_url, "Portfolio_Targets")

    @with_sheet_backoff
    def _rows():
        return ws.get_all_records()
    data = _rows()

    drift_alerts = []
    status_col_values = []  # for a single batch write to H2:H

    # Build statuses first, then update in one call
    for row in data:
        token   = (row.get("Token") or "").strip()
        target  = _to_float(row.get("Target %", 0))
        minpct  = _to_float(row.get("Min %", 0))
        maxpct  = _to_float(row.get("Max %", 100))
        current = _to_float(row.get("Current %", 0))

        status = "On target"
        if current < minpct:
            status = "Undersized"
            drift_alerts.append(f"🔽 {token}: {current}% (Target: {target}%)")
        elif current > maxpct:
            status = "Overweight"
            drift_alerts.append(f"🔼 {token}: {current}% (Target: {target}%)")

        status_col_values.append([status])

    # Batch update H2:H{n}
    if status_col_values:
        end_row = len(status_col_values) + 1
        rng = f"H2:H{end_row}"
        @with_sheet_backoff
        def _upd():
            ws.update(rng, status_col_values, value_input_option="USER_ENTERED")
        _upd()

    # If any drift found, ping once + set NovaTrigger
    if drift_alerts:
        body = "📊 <b>Portfolio Drift Detected!</b>\n\n" + "\n".join(drift_alerts) + \
               "\n\nReply YES to rebalance or SKIP to ignore."
        try:
            send_telegram_message(body)
            print("✅ Drift alert sent via Telegram")
        except Exception as e:
            print(f"⚠️ Telegram send error: {e}")

        # Lightweight trigger
        try:
            trig = _open_ws(sheet_url, "NovaTrigger")
            @with_sheet_backoff
            def _set_trig():
                trig.update_acell("A1", "REBALANCE ALERT")
            _set_trig()
            print("✅ NovaTrigger set to REBALANCE ALERT")
        except Exception as e:
            print(f"⚠️ Failed to update NovaTrigger: {e}")

    print("✅ Rebalance check complete.")
