# nova_trigger_watcher.py — reads NovaTrigger!A1 and routes manual commands
import os, time, random, gspread
from oauth2client.service_account import ServiceAccountCredentials
from nova_trigger import route_manual
from datetime import datetime

from utils import sheets_append_rows  # you already use this elsewhere (e.g., telemetry_mirror)

TAB    = os.getenv("NOVA_TRIGGER_TAB","NovaTrigger")
SHEET  = os.getenv("SHEET_URL")
JIT_MIN = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S","0.3"))
JIT_MAX = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S","1.2"))

def _open():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(SHEET)

NOVA_TRIGGER_LOG_TAB = os.environ.get("NOVA_TRIGGER_LOG_TAB", "NovaTrigger_Log")

def _append_novatrigger_log(
    trigger: str,
    status: str,
    policy_ok: bool,
    enq_ok: bool,
    reason: str,
) -> None:
    """
    Append a single row into NovaTrigger_Log:
      Timestamp | Trigger | Notes
    """
    sh = _open()
    ws = sh.worksheet(NOVA_TRIGGER_LOG_TAB)

    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    notes = f"status={status}; policy_ok={policy_ok}; enq_ok={enq_ok}; reason={reason}"

    rows = [[ts, trigger, notes]]
    sheets_append_rows(ws, rows)

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
    if raw.upper().startswith("MANUAL_REBUY"):
        out = route_manual(raw)

        decision = out.get("decision") or {}
        enqueue = out.get("enqueue") or {}

        policy_ok = bool(decision.get("ok"))
        enq_ok = bool(enqueue.get("ok"))

        status = "APPROVED" if policy_ok else "DENIED"
        reason = (
            enqueue.get("reason")
            or enqueue.get("error")
            or decision.get("reason")
            or "ok"
        )

        print(
            f"✅ Manual routed: policy_ok={policy_ok} "
            f"enq_ok={enq_ok} status={status} reason={reason}"
        )

        # Always log to NovaTrigger_Log
        try:
            _append_novatrigger_log(
                trigger=raw,
                status=status,
                policy_ok=policy_ok,
                enq_ok=enq_ok,
                reason=reason,
            )
        except Exception as e:
            # Log but don't break the watcher
            print(f"⚠ Failed to write NovaTrigger_Log row: {e!r}")

        # Only clear A1 if everything actually went through
        if policy_ok and enq_ok:
            ws.update_acell("A1", "")
            print("🧹 Cleared manual trigger after successful enqueue.")
        else:
            # Keep the trigger value so you can adjust / re-fire if needed
            print("⏸ Keeping manual trigger in A1 (policy or enqueue failed).")

        return

    # --- Non-manual triggers (SOS/FYI/etc.) --------------------------------
    # For everything that is NOT MANUAL_REBUY, keep the old behaviour:
    ws.update_acell("A1", "")
    print(f"🧹 Cleared non-manual trigger: {raw}")
