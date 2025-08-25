import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, safe_float, send_telegram_message_dedup

# --- 429-safe wrappers --------------------------------------------------------

@with_sheet_backoff
def _open_sheet(url):
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "sentiment-log-service.json", scope
    )
    return gspread.authorize(creds).open_by_url(url)

@with_sheet_backoff
def _get_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _batch_update(ws, start_a1, rows):
    # single RPC for all drift statuses
    ws.update(start_a1, rows, value_input_option="USER_ENTERED")

@with_sheet_backoff
def _update_acell(ws, a1, v):
    ws.update_acell(a1, v)

# --- main ---------------------------------------------------------------------

def run_rebalance_scanner():
    print("🔁 Running Rebalance Scanner...")
    try:
        sheet = _open_sheet(os.getenv("SHEET_URL"))
        ws = sheet.worksheet("Portfolio_Targets")

        data = _get_records(ws)

        drift_rows = []   # rows to write back (only column H content)
        drift_alerts = []

        # header row is row 1; records start at row 2; column H is the Drift Status
        # we'll build a contiguous write block H2:H{n} to minimize write calls
        for i, row in enumerate(data, start=2):
            token = (row.get("Token") or "").strip()
            target = safe_float(row.get("Target %"), 0.0)
            min_pct = safe_float(row.get("Min %"), 0.0)
            max_pct = safe_float(row.get("Max %"), 100.0)
            current = safe_float(row.get("Current %"), 0.0)

            if token == "":
                drift_rows.append([""])  # keep index aligned; blank row
                continue

            if current < min_pct:
                drift_status = "Undersized"
                drift_alerts.append(f"🔽 {token}: {current}% (Target: {target}%)")
            elif current > max_pct:
                drift_status = "Overweight"
                drift_alerts.append(f"🔼 {token}: {current}% (Target: {target}%)")
            else:
                drift_status = "On target"

            drift_rows.append([drift_status])

        if drift_rows:
            # H2 corresponds to the first record row
            _batch_update(ws, "H2", drift_rows)

        if drift_alerts:
            token_list = "\n".join(drift_alerts)
            message = (
                "📊 <b>Portfolio Drift Detected!</b>\n\n"
                f"{token_list}\n\n"
                "Reply YES to rebalance or SKIP to ignore."
            )
            # de‑dupe this message for 30 minutes under a stable key
            send_telegram_message_dedup(message, key="rebalance_alert", ttl_min=30)

            # flip NovaTrigger once (cheap single‑cell write)
            try:
                trig_ws = sheet.worksheet("NovaTrigger")
                _update_acell(trig_ws, "A1", "REBALANCE ALERT")
                print("✅ NovaTrigger set to REBALANCE ALERT")
            except Exception as e:
                print(f"⚠️ Failed to update NovaTrigger: {e}")

        print("✅ Rebalance check complete.")

    except Exception as e:
        print(f"❌ Rebalance scanner error: {e}")
