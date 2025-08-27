# performance_dashboard.py
import os
from statistics import median
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

try:
    from utils import with_sheet_backoff, str_or_empty, to_float
except Exception:
    def with_sheet_backoff(fn):
        def wrapper(*a, **k):
            return fn(*a, **k)
        return wrapper
    def str_or_empty(v):
        return str(v).strip() if v is not None else ""
    def to_float(v):
        s = str_or_empty(v).replace("%", "").replace(",", "")
        try:
            return float(s) if s != "" else None
        except Exception:
            return None

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def _get_sheet():
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_url(os.getenv("SHEET_URL"))

@with_sheet_backoff
def _ws(sheet, name):
    return sheet.worksheet(name)

def _safe_records(ws):
    hdr = ws.row_values(1)
    rows = ws.get_all_records()
    return hdr, rows

def run_performance_dashboard():
    sheet = _get_sheet()
    stats_ws = _ws(sheet, "Rotation_Stats")
    dash_ws  = _ws(sheet, "Performance_Dashboard")
    hb_ws    = _ws(sheet, "NovaHeartbeat")

    _, stats = _safe_records(stats_ws)

    total = 0
    yes_count = 0
    yes_perf = []  # numeric performance for YES only
    wins_yes = 0

    for r in stats:
        token = str_or_empty(r.get("Token"))
        if not token:
            continue
        total += 1

        decision = str_or_empty(r.get("Decision")).upper()
        perf_val = to_float(r.get("Performance"))

        if decision == "YES":
            yes_count += 1
            if perf_val is not None:
                yes_perf.append(perf_val)
                if perf_val > 0:
                    wins_yes += 1

    win_rate = (wins_yes / yes_count * 100.0) if yes_count > 0 else 0.0
    med_yes  = median(yes_perf) if yes_perf else 0.0

    # Heartbeat cell A2
    try:
        last_hb = str_or_empty(hb_ws.acell("A2").value)
    except Exception:
        last_hb = ""

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Compose a compact dashboard block; one atomic update
    values = [
        ["NovaTrade Performance Dashboard", ""],
        ["Last Dashboard Refresh", now_str],
        ["Last Heartbeat (A2)", last_hb],
        ["" , ""],
        ["Total Tokens", total],
        ["YES Count", yes_count],
        ["Win Rate (YES, %positive Performance)", f"{win_rate:.2f}%"],
        ["Median Performance (YES)", f"{med_yes:.2f}"],
    ]

    # Write starting at A1 (atomic)
    dash_ws.update("A1:B8", values, value_input_option="RAW")
    print("performance_dashboard: updated successfully (atomic write).")

if __name__ == "__main__":
    run_performance_dashboard()
