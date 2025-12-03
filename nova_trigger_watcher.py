# nova_trigger_watcher.py — reads NovaTrigger!A1 and routes manual commands
#
# Phase 19 version:
#   * Uses utils.get_ws + ws.append_row (no sheets_append_rows decorator).
#   * Logs policy/enqueue outcome to NovaTrigger_Log.
#   * Only clears A1 when both policy_ok and enq_ok are True.

import os
import time
import random
from datetime import datetime, timezone

from utils import get_ws, warn, info  # uses central Sheets gateway
from nova_trigger import route_manual

TAB = os.getenv("NOVA_TRIGGER_TAB", "NovaTrigger")
NOVA_TRIGGER_LOG_WS = os.getenv("NOVA_TRIGGER_LOG_WS", "NovaTrigger_Log")

JITTER_MIN = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S", "0.3"))
JITTER_MAX = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S", "1.2"))

if JITTER_MAX < JITTER_MIN:
    JITTER_MIN, JITTER_MAX = JITTER_MAX, JITTER_MIN


def _append_novatrigger_log(
    trigger: str, policy_ok: bool, enq_ok: bool, reason: str, mode: str
) -> None:
    """
    Append a single log row to NovaTrigger_Log:
      Timestamp | Raw Trigger | Notes
    """
    try:
        ws_log = get_ws(NOVA_TRIGGER_LOG_WS)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        notes = f"policy_ok={policy_ok}; enq_ok={enq_ok}; mode={mode}; reason={reason}"
        ws_log.append_row(
            [ts, trigger, notes],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        warn(f"NovaTrigger log append failed: {e}")


def check_nova_trigger() -> None:
    """
    Main entrypoint: jitter, read NovaTrigger!A1, route MANUAL_REBUY, log outcome.
    """
    # Respect jitter to avoid Sheets thundering-herd
    time.sleep(random.uniform(JITTER_MIN, JITTER_MAX))

    ws = get_ws(TAB)
    raw = (ws.acell("A1").value or "").strip()
    if not raw:
        info(f"{TAB} empty; no trigger.")
        return

    # --- MANUAL_REBUY flow -------------------------------------------------
    if raw.upper().startswith("MANUAL_REBUY"):
        out = route_manual(raw) or {}
        decision = out.get("decision") or {}
        enqueue = out.get("enqueue") or {}
        policy_ok = bool(decision.get("ok"))
        enq_ok = bool(enqueue.get("ok"))

        mode = str(out.get("mode") or os.getenv("REBUY_MODE", "dryrun")).lower()
        reason = (
            enqueue.get("reason")
            or enqueue.get("error")
            or decision.get("reason")
            or "ok"
        )

        info(
            f"Manual routed: policy_ok={policy_ok} "
            f"enq_ok={enq_ok} mode={mode} reason={reason}"
        )

        try:
            _append_novatrigger_log(raw, policy_ok, enq_ok, reason, mode)
        except Exception as e:
            warn(f"Failed to write NovaTrigger_Log row: {e!r}")

        # Only clear A1 if everything actually went through
        if policy_ok and enq_ok:
            ws.update_acell("A1", "")
            info("Cleared manual trigger after successful enqueue.")
        else:
            info(
                "Keeping manual trigger in A1 (policy or enqueue failed); "
                "adjust/retry as needed."
            )

        return

    # --- Non-manual triggers (SOS/FYI/etc.) --------------------------------
    # For everything that is NOT MANUAL_REBUY, keep the old behaviour: clear after use.
    ws.update_acell("A1", "")
    info(f"Cleared non-manual trigger: {raw}")
