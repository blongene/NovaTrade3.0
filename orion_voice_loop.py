# orion_voice_loop.py
import os
import time
import json
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import with_sheet_backoff, send_telegram_message_dedup

SHEET_URL = os.environ.get("SHEET_URL")
STATE_FILE = "/tmp/orion_voice_state.json"   # remembers last trigger + when it was sent
CHECK_INTERVAL_SEC = int(os.getenv("ORION_CHECK_INTERVAL_SEC", "60"))
ORION_TTL_MIN = int(os.getenv("ORION_TTL_MIN", "120"))  # dedupe window per trigger

# ---------- helpers ----------
def _load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {"last_value": "READY", "last_sent_ts": 0}

def _save_state(s):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(s, f)
    except Exception:
        pass

def _build_message_from_trigger(trigger_value: str) -> str:
    t = (trigger_value or "").upper()
    if t == "ROTATION COMPLETE":
        return ("📥 *Rotation Execution Confirmed*\n\n"
                "✅ New token(s) added to active tracking.\n"
                "ROI monitoring has begun.\n"
                "Loop closed. Orion is watching.")
    if t == "NOVA UPDATE":
        return ("📡 *NovaTrade System Online*\n"
                "All modules are active.\n"
                "You will be notified if input is needed or a token stalls.")
    if t == "SOS":
        return ("🆘 *NovaTrade Alert*\n"
                "A system error or webhook failure was detected.\n"
                "Please check the system log immediately.")
    if t == "SYNC NEEDED":
        return ("🧠 *Sync Required*\n"
                "New decisions are pending rotation. Please review the planner tab.")
    if t == "FYI ONLY":
        return ("🔔 *FYI Notification*\n"
                "This is a passive update. No action is required.")
    return (f"🔔 *NovaTrade Alert*\n"
            f"Trigger: `{t}` received.\n"
            "Check the system dashboard for details.")

def _now_epoch():
    return int(time.time())

# ---------- Sheets ----------
@with_sheet_backoff
def _open_trigger_ws():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet("NovaTrigger")

@with_sheet_backoff
def _read_trigger(ws):
    try:
        v = ws.acell("A1").value or ""
        return v.strip().upper()
    except Exception:
        return ""

@with_sheet_backoff
def _reset_trigger(ws):
    try:
        ws.update_acell("A1", "READY")
    except Exception as e:
        print(f"⚠️ Failed to reset NovaTrigger A1 → READY: {e}")

# ---------- core ----------
def check_nova_trigger_and_ping():
    state = _load_state()
    try:
        ws = _open_trigger_ws()
        value = _read_trigger(ws)

        # Nothing to do
        if not value or value == "READY":
            return

        # If we saw this same trigger recently, skip (belt & suspenders on top of TG dedupe)
        last_value = state.get("last_value", "READY")
        last_sent_ts = int(state.get("last_sent_ts", 0))
        age_min = ( _now_epoch() - last_sent_ts ) / 60.0

        if value == last_value and age_min < ORION_TTL_MIN:
            # still within our local cooldown for identical trigger
            return

        # Build message + send via global de-dupe (keyed by value)
        msg = _build_message_from_trigger(value)
        dedup_key = f"nova_trigger:{value}"
        send_telegram_message_dedup(msg, key=dedup_key, ttl_min=ORION_TTL_MIN)

        # Persist state and reset the trigger cell
        state["last_value"] = value
        state["last_sent_ts"] = _now_epoch()
        _save_state(state)
        _reset_trigger(ws)

        print(f"🔔 Orion voice triggered (sent): {value} @ {datetime.utcnow().isoformat()}Z")

    except Exception as e:
        print(f"❌ Error in check_nova_trigger_and_ping: {e}")

def run_orion_voice_loop():
    # Simple forever loop; Render will keep the process alive
    while True:
        check_nova_trigger_and_ping()
        time.sleep(CHECK_INTERVAL_SEC)

if __name__ == "__main__":
    run_orion_voice_loop()
