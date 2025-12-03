# nova_trigger_watcher.py — reads NovaTrigger!A1 and routes manual commands

import os
import random
import time
from datetime import datetime, timezone

from nova_trigger import route_manual
from utils import get_ws  # <-- only use get_ws; no sheets_append_rows here

TAB = os.getenv("NOVA_TRIGGER_TAB", "NovaTrigger")
LOG_TAB = os.getenv("NOVA_TRIGGER_LOG_TAB", "NovaTrigger_Log")
JIT_MIN = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S", "0.3"))
JIT_MAX = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S", "1.2"))


def _append_novatrigger_log(trigger: str, policy_ok: bool, enq_ok: bool, reason: str) -> None:
    """
    Best-effort append into NovaTrigger_Log:

      A: timestamp (UTC)
      B: raw trigger string
      C: notes (policy_ok / enq_ok / reason)
    """
    try:
        ws_log = get_ws(LOG_TAB)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        notes = f"policy_ok={policy_ok}; enq_ok={enq_ok}; reason={reason}"

        # gspread's append_row signature: append_row(list_of_values, value_input_option='RAW', insert_data_option=None, table_range=None)
        ws_log.append_row([ts, trigger, notes], value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"⚠ NovaTrigger log append failed: {e!r}")


def check_nova_trigger() -> None:
    print("▶ Nova trigger check …")

    # jitter so multiple jobs don't slam Sheets at once
    try:
        delay = random.uniform(JIT_MIN, JIT_MAX)
        time.sleep(delay)
    except Exception:
        pass

    ws = get_ws(TAB)
    raw = (ws.acell("A1").value or "").strip()
    if not raw:
        print(f"ℹ️ {TAB} empty; no trigger.")
        return

    # Manual rebuy commands
    if raw.upper().startswith("MANUAL_REBUY"):
        out = route_manual(raw)
        decision = out.get("decision") or {}
        enqueue = out.get("enqueue") or {}

        policy_ok = bool(decision.get("ok"))
        enq_ok = bool(enqueue.get("ok"))
        reason = enqueue.get("reason") or decision.get("reason") or "n/a"

        print(f"✅ Manual routed: policy_ok={policy_ok} enq_ok={enq_ok} reason={reason}")

        # Always try to log, even if policy/enqueue failed
        _append_novatrigger_log(raw, policy_ok, enq_ok, reason)

        # Clear trigger only if it actually enqueued
        if policy_ok and enq_ok:
            ws.update_acell("A1", "")
            print("🧹 Cleared manual trigger after successful enqueue.")
        else:
            print("⏸ Keeping manual trigger in A1 (policy or enqueue failed).")

        return

    # Non-manual triggers (SOS/FYI/etc.): just clear after a ping
    ws.update_acell("A1", "")
    print(f"ℹ️ Cleared non-manual trigger: {raw}")


if __name__ == "__main__":
    check_nova_trigger()
