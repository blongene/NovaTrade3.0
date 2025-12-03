"""
Nova Trigger Watcher

Polls the NovaTrigger tab for commands and routes them through nova_trigger.route_manual.
Also appends an audit row into NovaTrigger_Log.
"""

from __future__ import annotations

import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict

from nova_trigger import route_manual
from utils import SHEET_URL, get_ws, get_ws_cached, sheets_append_rows, with_backoff

TAB = os.getenv("NOVA_TRIGGER_TAB", "NovaTrigger")
LOG_TAB = os.getenv("NOVA_TRIGGER_LOG_TAB", "NovaTrigger_Log")

# Small random jitter so multiple jobs don't hammer Sheets in lock-step
JIT_MIN_S = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S", "3"))
JIT_MAX_S = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S", "8"))


def _append_novatrigger_log(
    *,
    trigger: str,
    status: str,
    policy_ok: bool,
    enq_ok: bool,
    reason: str,
) -> None:
    """
    Append an audit row into NovaTrigger_Log.

    Columns (A-C):
      A: Timestamp (UTC ISO)
      B: Trigger string (raw A1 contents)
      C: Notes (status / policy_ok / enq_ok / reason)
    """
    ws = get_ws_cached(LOG_TAB)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    notes = (
        f"status={status}; "
        f"policy_ok={policy_ok}; "
        f"enq_ok={enq_ok}; "
        f"reason={reason}"
    )

    rows = [[ts, trigger, notes]]
    sheets_append_rows(ws, rows)


@with_backoff
def check_nova_trigger() -> None:
    print("▶ Nova trigger check …")

    # Jitter per run
    time.sleep(random.uniform(JIT_MIN_S, JIT_MAX_S))

    ws = get_ws(TAB)

    raw = (ws.acell("A1").value or "").strip()
    if not raw:
        print(f"🟦 {TAB} empty; no trigger.")
        return

    # Manual rebuy path
    if raw.upper().startswith("MANUAL_REBUY"):
        out: Dict[str, Any] = route_manual(raw)

        decision = out.get("decision") or {}
        policy_ok = bool(decision.get("ok"))
        status = decision.get("status", "UNKNOWN")

        enqueue = out.get("enqueue") or {}
        enq_ok = bool(enqueue.get("ok"))
        reason = enqueue.get("reason") or decision.get("reason") or "n/a"

        print(
            f"✅ Manual routed: policy_ok={policy_ok} "
            f"enq_ok={enq_ok} status={status} reason={reason}"
        )

        # Always log to NovaTrigger_Log – even if policy or enqueue failed
        try:
            _append_novatrigger_log(
                trigger=raw,
                status=status,
                policy_ok=policy_ok,
                enq_ok=enq_ok,
                reason=reason,
            )
        except Exception as e:  # pragma: no cover – best-effort
            print(f"⚠ NovaTrigger log append failed: {e!r}")

        # Only clear A1 when the intent *actually* made it into the queue
        if policy_ok and enq_ok:
            ws.update_acell("A1", "")
            print("🧹 Cleared manual trigger after successful enqueue.")
        else:
            print("⏸ Keeping manual trigger in A1 (policy or enqueue failed).")

        return

    # Non-manual triggers (SOS / FYI / etc) keep the old behaviour:
    ws.update_acell("A1", "")
    print(f"🧹 Cleared non-manual trigger: {raw}")


if __name__ == "__main__":
    check_nova_trigger()
