# nova_trigger_watcher.py — reads NovaTrigger!A1 and routes manual commands
#
# Phase 19+ hardened version:
#   * Uses utils.get_ws + append_row (central Sheets gateway).
#   * Logs policy/enqueue outcome to NovaTrigger_Log.
#   * Only clears A1 when both policy_ok and enq_ok are True.
#   * Safe import of nova_trigger.route_manual (no hard crash on deploy).
#   * Respects NT_ALLOW_MANUAL env flag.

import os
import time
import random
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from utils import get_ws, warn, info  # central Sheets gateway

# --- Config -----------------------------------------------------------------

TAB = os.getenv("NOVA_TRIGGER_TAB", "NovaTrigger")
NOVA_TRIGGER_LOG_WS = os.getenv("NOVA_TRIGGER_LOG_WS", "NovaTrigger_Log")

JITTER_MIN = float(os.getenv("NOVA_TRIGGER_JITTER_MIN_S", "0.3"))
JITTER_MAX = float(os.getenv("NOVA_TRIGGER_JITTER_MAX_S", "1.2"))

if JITTER_MAX < JITTER_MIN:
    JITTER_MIN, JITTER_MAX = JITTER_MAX, JITTER_MIN

NT_ALLOW_MANUAL = os.getenv("NT_ALLOW_MANUAL", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# --- Safe import of route_manual --------------------------------------------

_route_manual = None  # type: Optional[callable]

try:
    from nova_trigger import route_manual as _route_manual  # type: ignore
    if not callable(_route_manual):
        warn("nova_trigger_watcher: route_manual imported but is not callable.")
        _route_manual = None
    else:
        info("nova_trigger_watcher: route_manual imported successfully.")
except Exception as e:
    warn(f"nova_trigger_watcher: route_manual not available: {e!r}")
    _route_manual = None


# --- Helpers ----------------------------------------------------------------


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
        notes = (
            f"policy_ok={policy_ok}; enq_ok={enq_ok}; mode={mode}; reason={reason}"
        )
        ws_log.append_row(
            [ts, trigger, notes],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        warn(f"NovaTrigger log append failed: {e}")


def _manual_disabled_reason() -> str:
    if not NT_ALLOW_MANUAL:
        return "manual_disabled_by_env"
    if _route_manual is None:
        return "route_manual_unavailable"
    return "unknown"


# --- Main entrypoint --------------------------------------------------------


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
        # Safety checks first
        if not NT_ALLOW_MANUAL:
            reason = "manual_disabled_by_env"
            warn(
                f"NovaTrigger manual ignored: NT_ALLOW_MANUAL=0; trigger={raw!r}"
            )
            try:
                _append_novatrigger_log(
                    raw,
                    policy_ok=False,
                    enq_ok=False,
                    reason=reason,
                    mode=os.getenv("REBUY_MODE", "dryrun"),
                )
            except Exception as e:
                warn(f"Failed to write NovaTrigger_Log row (manual disabled): {e!r}")
            # We KEEP A1 intact so the operator can re-issue after enabling.
            return

        if _route_manual is None:
            reason = "route_manual_unavailable"
            warn(
                f"NovaTrigger manual ignored: route_manual not importable; trigger={raw!r}"
            )
            try:
                _append_novatrigger_log(
                    raw,
                    policy_ok=False,
                    enq_ok=False,
                    reason=reason,
                    mode=os.getenv("REBUY_MODE", "dryrun"),
                )
            except Exception as e:
                warn(
                    f"Failed to write NovaTrigger_Log row (route_manual missing): {e!r}"
                )
            # Keep A1 so we don't silently drop the operator's command.
            return

        # Happy path: route via nova_trigger.route_manual
        try:
            out: Dict[str, Any] = _route_manual(raw) or {}
        except Exception as e:
            reason = f"route_manual_exception:{e}"
            warn(f"NovaTrigger manual routing error: {e!r}")
            try:
                _append_novatrigger_log(
                    raw,
                    policy_ok=False,
                    enq_ok=False,
                    reason=reason,
                    mode=os.getenv("REBUY_MODE", "dryrun"),
                )
            except Exception as e2:
                warn(f"Failed to write NovaTrigger_Log row (exception): {e2!r}")
            # Keep A1 to allow operator to fix and retry.
            return

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
