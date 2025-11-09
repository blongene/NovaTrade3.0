
# rotation_signal_engine.py — self-contained (no send_telegram dependency)
# Milestone prompts driven by Days Held only. Writes to ROI_Review_Log.
# Telegram sending: tries utils helpers, else direct Bot API via env.
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")
MILESTONES = [3, 7, 14, 30]

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

def _safe_int(v, default=0):
    try:
        return int(str(v).strip())
    except Exception:
        return default

def _safe_float(v):
    try:
        s = str(v).replace("%","").replace(",","").strip()
        if s == "" or s.upper() == "N/A":
            return None
        return float(s)
    except Exception:
        return None

def _send_telegram_markdown(text: str):
    # Try project helpers first
    try:
        from utils import send_telegram_message_dedup as _send
        _send(text, key=f"milestone:{hash(text)}")
        return
    except Exception:
        pass
    try:
        from utils import send_telegram_message as _send2
        _send2(text)
        return
    except Exception:
        pass
    # Bare fallback (optional)
    if BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
                timeout=8,
            )
        except Exception:
            pass

def run_milestone_alerts(token_override=None):
    if not SHEET_URL:
        print("[Milestones] SHEET_URL missing; abort.")
        return
    print("⏱  Milestone scan (Days Held)…")

    sh = _open_sheet()
    log_ws = sh.worksheet("Rotation_Log")
    try:
        review_ws = sh.worksheet("ROI_Review_Log")
    except gspread.exceptions.WorksheetNotFound:
        review_ws = sh.add_worksheet(title="ROI_Review_Log", rows=1000, cols=12)
        review_ws.append_row(
            ["Timestamp","Token","Days Held","Follow-up ROI","Initial ROI","Final ROI","Re-Vote","Feedback","Synced?","Would You Say YES Again?"],
            value_input_option="USER_ENTERED"
        )

    rows = log_ws.get_all_records()
    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    sent = 0
    for r in rows:
        token = str(r.get("Token","")).strip().upper()
        if token_override and token != str(token_override).strip().upper():
            continue

        days_held = _safe_int(r.get("Days Held", 0), 0)
        if not token or days_held <= 0 or days_held not in MILESTONES:
            continue

        fup_roi = _safe_float(r.get("Follow-up ROI"))
        init_roi = _safe_float(r.get("Initial ROI"))

        # Append review row
        review_ws.append_row(
            [now_iso, token, days_held,
             "" if fup_roi is None else f"{fup_roi:.2f}%",
             "" if init_roi is None else f"{init_roi:.2f}%",
             "", "", "", "", ""],
            value_input_option="USER_ENTERED"
        )

        # Telegram message
        msg = (
            f"⏳ *ROI Milestone: {token}*\n"
            f"• Days Held: *{days_held}*\n"
            f"• Follow-up ROI: *{'' if fup_roi is None else f'{fup_roi:.2f}%'}*\n"
            f"• Initial ROI: *{'' if init_roi is None else f'{init_roi:.2f}%'}*\n\n"
            f"Would you still vote *YES* today?"
        )
        _send_telegram_markdown(msg)
        sent += 1

    print(f"✅ Milestone alerts complete. Prompts sent: {sent}")
    return sent

# Back-compat alias
def scan_rotation_candidates(token_override=None, *args, **kwargs):
    return run_milestone_alerts(token_override=token_override)
