# authority_gate.py
from enum import Enum

class Decision(str, Enum):
    AUTO = "AUTO"
    HUMAN_REQUIRED = "HUMAN_REQUIRED"
    HARD_STOP = "HARD_STOP"


def evaluate(intent: dict, context: dict) -> Decision:
    t = intent.get("type")
    mode = intent.get("mode")
    phase = intent.get("phase")

    # --- GLOBAL HARD STOPS ---
    if context.get("cloud_hold"):
        return Decision.HARD_STOP

    if context.get("telemetry_stale_sec", 0) > 600:
        return Decision.HARD_STOP

    # --- ALWAYS SAFE ---
    if t in {"NOTE", "BALANCE_SNAPSHOT", "TELEMETRY_PUSH"}:
        return Decision.AUTO

    # --- DRYRUN TRADES ---
    if t == "TRADE" and mode == "dryrun":
        return Decision.AUTO

    # --- LIVE TRADES ---
    if t == "TRADE" and mode == "live":
        confidence = context.get("confidence", 0)
        amount = context.get("amount_usd", 0)
        first_time = context.get("first_time_symbol", True)

        if first_time:
            return Decision.HUMAN_REQUIRED

        if confidence < 0.75:
            return Decision.HUMAN_REQUIRED

        if amount > 25:
            return Decision.HUMAN_REQUIRED

        return Decision.AUTO

    # --- EVERYTHING ELSE ---
    return Decision.HUMAN_REQUIRED
