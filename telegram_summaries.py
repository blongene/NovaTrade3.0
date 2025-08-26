# telegram_summaries.py
import os
import gspread
from datetime import datetime, timezone
from oauth2client.service_account import ServiceAccountCredentials
from utils import with_sheet_backoff, send_once_per_day

SHEET_URL = os.getenv("SHEET_URL")

def _gspread():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(SHEET_URL)

@with_sheet_backoff
def _get_all_records(ws):
    return ws.get_all_records()

def _fmt_summary(pending:int, yes:int, no:int, skip:int) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return (
        f"🧾 <b>Daily Summary — {today} (UTC)</b>\n"
        f"⏳ Pending Rotations: <b>{pending}</b>\n"
        f"🗳 Feedback — YES:{yes}  NO:{no}  SKIP:{skip}"
    )

def run_telegram_summary():
    sh = _gspread()
    stats = _get_all_records(sh.worksheet("Rotation_Stats"))

    pending = sum(1 for r in stats if str(r.get("Status","")).upper() == "PENDING")
    yes = sum(1 for r in stats if str(r.get("Decision","")).upper() == "YES")
    no  = sum(1 for r in stats if str(r.get("Decision","")).upper() == "NO")
    skip= sum(1 for r in stats if str(r.get("Decision","")).upper() == "SKIP")

    msg = _fmt_summary(pending, yes, no, skip)
    # once per UTC day; no spam on reboots
    send_once_per_day("daily_summary", msg)
