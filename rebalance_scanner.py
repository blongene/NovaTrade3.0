import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import with_sheet_backoff, get_sheet, safe_float, send_telegram_message_dedup

# ---------- sheet helpers ----------
@with_sheet_backoff
def _ws_get_all_records(ws):
    return ws.get_all_records()

@with_sheet_backoff
def _ws_update_range(ws, a1_range, values_2d):
    ws.update(a1_range, values_2d, value_input_option="USER_ENTERED")

@with_sheet_backoff
def _ws_update_acell(ws, a1, v):
    ws.update_acell(a1, v)

def _fmt_pct(x):
    try:
        return f"{float(x):g}%"
    except Exception:
        return str(x)

def run_rebalance_scanner():
    print("🔁 Running Rebalance Scanner...")

    try:
        sh = get_sheet()
        ws = sh.worksheet("Portfolio_Targets")

        records = _ws_get_all_records(ws)  # single read
        if not records:
            print("ℹ️ Portfolio_Targets empty; nothing to scan.")
            return

        # column H = Drift Status (assumed by your sheet)
        start_row = 2
        end_row = start_row + len(records) - 1
        drift_col = "H"
        out_rows = []        # for batch write
        drift_alerts = []    # for Telegram

        for i, row in enumerate(records, start=start_row):
            token  = (row.get("Token") or "").strip()
            target = safe_float(row.get("Target %"), 0.0)
            min_p  = safe_float(row.get("Min %"), 0.0)
            max_p  = safe_float(row.get("Max %"), 100.0)
            curr   = safe_float(row.get("Current %"), 0.0)

            status = "On target"
            if curr < min_p:
                status = "Undersized"
                drift_alerts.append(f"🔽 {token}: {curr}% (Target {target}%)")
            elif curr > max_p:
                status = "Overweight"
                drift_alerts.append(f"🔼 {token}: {curr}% (Target {target}%)")

            out_rows.append([status])

        # single batch write for H2:H{end}
        rng = f"{drift_col}{start_row}:{drift_col}{end_row}"
        _ws_update_range(ws, rng, out_rows)

        if drift_alerts:
            msg = "📊 <b>Portfolio Drift Detected</b>\n\n" + "\n".join(drift_alerts) + "\n\nReply YES to rebalance or SKIP to ignore."
            # de-dupe for 30 minutes under a stable key
            send_telegram_message_dedup(msg, key="rebalance:drift", ttl_min=30)
            # nudge NovaTrigger (cheap, single cell)
            try:
                _ws_update_acell(sh.worksheet("NovaTrigger"), "A1", "REBALANCE ALERT")
            except Exception as e:
                print(f"⚠️ NovaTrigger update skipped: {e}")

        print("✅ Rebalance check complete.")

    except Exception as e:
        print(f"❌ Rebalance scanner error: {e}")
