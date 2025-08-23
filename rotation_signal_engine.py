# ✅ Patched rotation_signal_engine.py with ROI parsing safety

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os
from send_telegram import send_rotation_alert

PROMPT_MEMORY = {}
MILESTONES = [3, 7, 14, 30]

def scan_rotation_candidates(token_override=None, *args, **kwargs):
    """
    If token_override is given, restrict the scan to that token/symbol.
    Other args/kwargs are accepted for backward compatibility.
    """
    # ... your current code that builds candidates list ...
    candidates = _load_candidates_somehow()

    if token_override:
        token_u = str(token_override).strip().upper()
        candidates = [c for c in candidates if str(c.get("Token","")).upper() == token_u]

    return candidates

def run_milestone_alerts():
    print("\U0001F6A7 Scanning for milestone alerts...")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(os.getenv("SHEET_URL"))

    log_ws = sheet.worksheet("Rotation_Log")
    review_ws = sheet.worksheet("ROI_Review_Log")

    log_data = log_ws.get_all_records()
    review_data = review_ws.get_all_records()

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for row in log_data:
        token = row.get("Token", "").strip()
        timestamp = row.get("Timestamp", "").strip()
        roi_text = str(row.get("Follow-up ROI", "")).strip()

        try:
            days_held = int(row.get("Days Held", 0))
        except:
            print(f"⚠️ Invalid Days Held for {token}: {row.get('Days Held')}")
            continue

        # Validate ROI format
        if not token or not timestamp or "d since vote" not in roi_text:
            continue

        try:
            roi_days = int(roi_text.split("d")[0])
        except (ValueError, IndexError):
            print(f"⚠️ Malformed ROI text for {token}: {roi_text}")
            continue

        if days_held in MILESTONES and token not in PROMPT_MEMORY:
            print(f"✨ Milestone hit: {token} — {days_held}d")

            if any(r["Token"].strip() == token and int(r.get("Days Held", 0)) == days_held for r in review_data):
                continue

            new_row = [
                now,
                token,
                days_held,
                roi_text,
                "",  # Final ROI
                "",  # Re-Vote
                "",  # Feedback
                "",  # Synced?
                ""   # Would You Say YES Again?
            ]
            review_ws.append_row(new_row, value_input_option="USER_ENTERED")

            msg = f"⏳ *ROI Milestone Reached: {token}*\n– Days Held: {days_held}\n– ROI: {roi_text}\n\nWould you still vote YES today?"
            send_rotation_alert(token, msg)
            PROMPT_MEMORY[token] = True

    print("✅ Milestone alert scan complete.")
