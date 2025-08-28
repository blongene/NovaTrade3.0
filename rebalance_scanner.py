# rebalance_scanner.py
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, safe_float, send_telegram_message_dedup

SHEET_URL = os.getenv("SHEET_URL")
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

@with_sheet_backoff
def _open_ws(title: str):
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet(title)

def run_rebalance_scanner():
    print("▶️ Rebalance scanner …")
    try:
        ws = _open_ws("Portfolio_Targets")

        # Pull all values once
        vals = ws.get_all_values()
        if not vals:
            print("⚠️ Empty Portfolio_Targets sheet.")
            return

        header = vals[0]
        # Required columns & their indices (1-based for gspread .update_cell; 0-based for lists)
        def _col_ix(name):
            try:
                return header.index(name)
            except ValueError:
                return None

        token_ix     = _col_ix("Token")
        target_ix    = _col_ix("Target %")
        min_ix       = _col_ix("Min %")
        max_ix       = _col_ix("Max %")
        current_ix   = _col_ix("Current %")
        drift_ix     = _col_ix("Drift")  # we write here (create if missing)

        missing = [n for n,ix in [
            ("Token",token_ix),("Target %",target_ix),("Min %",min_ix),
            ("Max %",max_ix),("Current %",current_ix)
        ] if ix is None]
        if missing:
            print(f"⚠️ Portfolio_Targets missing columns: {', '.join(missing)}")
            return

        # Add Drift header if missing
        if drift_ix is None:
            header.append("Drift")
            ws.update("A1", [header])  # write whole header row once
            drift_ix = len(header) - 1  # 0-based index after append

        # Compute drifts
        updates = []
        alerts  = []
        for r_idx, row in enumerate(vals[1:], start=2):  # start=2 because header is row 1
            token = (row[token_ix] if token_ix < len(row) else "").strip().upper()
            if not token:
                continue

            t = safe_float(row[target_ix] if target_ix < len(row) else "", 0.0)
            mn = safe_float(row[min_ix]    if min_ix    < len(row) else "", 0.0)
            mx = safe_float(row[max_ix]    if max_ix    < len(row) else "", 0.0)
            cur= safe_float(row[current_ix]if current_ix< len(row) else "", 0.0)

            drift = cur - t  # simple drift in percentage points
            # Status messaging (optional): out-of-band alert if out of band
            out_of_band = (cur < mn) or (cur > mx)

            # Build a1 for Drift cell (NO duplicated sheet name; just column+row)
            # drift_ix is 0-based; add 1 to get column number for A1.
            col_num = drift_ix + 1

            # Convert column number → letters for A1
            def _a1_col(n):
                s = ""
                while n:
                    n, rem = divmod(n - 1, 26)
                    s = chr(65 + rem) + s
                return s

            a1 = f"{_a1_col(col_num)}{r_idx}"
            updates.append({"range": a1, "values": [[f"{drift:.2f}%"]]})

            if out_of_band:
                alerts.append(f"• {token}: {cur:.2f}% (band {mn:.2f}%–{mx:.2f}%, target {t:.2f}%)")

        # Batch write all drift cells in one call
        if updates:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            print(f"✅ Drift column updated for {len(updates)} row(s).")

        # Single de-duped Telegram ping if we have any out-of-band items
        if alerts:
            body = "📊 <b>Rebalance Drift Alert</b>\n" + "\n".join(alerts)
            send_telegram_message_dedup(body, key="rebalance_drift", ttl_min=30)

    except Exception as e:
        print(f"❌ Rebalance scanner error: {e}")
