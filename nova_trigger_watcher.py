# nova_trigger_watcher.py — reads NovaTrigger!A1 and routes manual commands
import os, time, random, gspread
from oauth2client.service_account import ServiceAccountCredentials
from nova_trigger import route_manual
from datetime import datetime
from sheets_gateway import get_ws_cached, sheets_append_rows
from utils import SHEET_URL, get_ws, get_ws_cached, sheets_append_rows

TAB    = os.getenv("NOVA_TRIGGER_TAB","NovaTrigger")
SHEET  = os.getenv("SHEET_URL")
NOVA_TRIGGER_JITTER_MIN_S = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S", "3"))
NOVA_TRIGGER_JITTER_MAX_S = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S", "9"))

if NOVA_TRIGGER_JITTER_MAX_S < NOVA_TRIGGER_JITTER_MIN_S:
    NOVA_TRIGGER_JITTER_MIN_S, NOVA_TRIGGER_JITTER_MAX_S = (
        NOVA_TRIGGER_JITTER_MAX_S,
        NOVA_TRIGGER_JITTER_MIN_S,
    )
def _open():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(SHEET)

from datetime import datetime, timezone

NOVA_TRIGGER_LOG_TAB = "NovaTrigger_Log"
SHEET_URL = os.environ["SHEET_URL"]

def _append_novatrigger_log(trigger: str, notes: str) -> None:
    """
    Best-effort append to NovaTrigger_Log.
    Does NOT raise – failures are only printed so we don't break the watcher.
    """
    try:
        ws = get_ws_cached(NOVA_TRIGGER_LOG_TAB, SHEET_URL)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = [[ts, trigger, notes]]
        sheets_append_rows(ws, rows)
    except Exception as e:
        # Don’t crash the watcher if logging fails
        print(f"⚠ NovaTrigger log append failed: {e!r}")

def check_nova_trigger() -> None:
    print("▶ Nova trigger check …")
    # keep your existing jitter constants
    time.sleep(random.uniform(NOVA_TRIGGER_JITTER_MIN_S, NOVA_TRIGGER_JITTER_MAX_S))

    sh = _open()
    ws = sh.worksheet(TAB)

    raw = (ws.acell("A1").value or "").strip()
    if not raw:
        print(f"🔹 {TAB} empty; no trigger.")
        return

    # --- MANUAL_REBUY flow -------------------------------------------------
    if raw.upper().startsWith("MANUAL_REBUY"):
    out = route_manual(raw)

    decision = out.get("decision", {}) or {}
    enqueue  = out.get("enqueue", {}) or {}

    policy_ok = bool(decision.get("ok"))
    enq_ok    = bool(enqueue.get("ok"))
    enq_reason = enqueue.get("reason", "ok")
    mode      = decision.get("mode", "live")

    notes = (
        f"status=APPROVED; ok={policy_ok}; "
        f"mode={mode}; enq_ok={enq_ok}; enq_reason={enq_reason}"
    )
    _append_novatrigger_log(raw, notes)

    print(f"✅ Manual routed: policy_ok={policy_ok} enq={enq_ok}")

    # clear trigger cell after handling manual command
    ws.update_acell("A1", "")
    return

    # --- Non-manual triggers (SOS/FYI/etc.) --------------------------------
    # For everything that is NOT MANUAL_REBUY, keep the old behaviour:
    ws.update_acell("A1", "")
    print(f"🧹 Cleared non-manual trigger: {raw}")
