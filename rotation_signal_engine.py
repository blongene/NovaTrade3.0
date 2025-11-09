
# rotation_signal_engine.py — milestone alerts driven by Days Held (numeric-safe)
# Removes dependency on ROI text like "7d since vote". Uses Days Held milestones only.

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
from send_telegram import send_rotation_alert

PROMPT_MEMORY = {}
MILESTONES = [3, 7, 14, 30]

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet_url = os.getenv("SHEET_URL")
    if not sheet_url:
        raise ValueError("SHEET_URL not set.")
    return client.open_by_url(sheet_url)

def scan_rotation_candidates(token_override=None, *args, **kwargs):
    # Back-compat stub; most logic is in run_milestone_alerts()
    return run_milestone_alerts(token_override=token_override)

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

def run_milestone_alerts(token_override=None):
    print("🚧 Scanning for milestone alerts (Days Held)…")
    sh = _open_sheet()
    log_ws = sh.worksheet("Rotation_Log")

    # ROI_Review_Log (create if missing) for appending prompts
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
        if not token or days_held <= 0:
            continue

        # Trigger prompt exactly at milestone days and only once per token (memory guard)
        if days_held in MILESTONES and token not in PROMPT_MEMORY:
            fup_roi = _safe_float(r.get("Follow-up ROI"))
            init_roi = _safe_float(r.get("Initial ROI"))

            # Append a clean row to ROI_Review_Log (numeric-friendly)
            review_ws.append_row(
                [now_iso, token, days_held, "" if fup_roi is None else f"{fup_roi:.2f}%",
                 "" if init_roi is None else f"{init_roi:.2f}%", "", "", "", "", ""],
                value_input_option="USER_ENTERED"
            )

            msg = (
                f"⏳ *ROI Milestone Reached: {token}*\n"
                f"• Days Held: *{days_held}*\n"
                f"• Follow-up ROI: *{'' if fup_roi is None else f'{fup_roi:.2f}%'}*\n"
                f"• Initial ROI: *{'' if init_roi is None else f'{init_roi:.2f}%'}*\n\n"
                f"Would you still vote *YES* today?"
            )
            send_rotation_alert(token, msg)
            PROMPT_MEMORY[token] = True
            sent += 1

    print(f"✅ Milestone alert scan complete. Prompts sent: {sent}")
