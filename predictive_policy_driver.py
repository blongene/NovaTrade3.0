
# predictive_policy_driver.py — Phase 10 scaffolding
# Applies Policy_Bias to incoming intents: scales notional/amount within guardrails,
# adds 'policy_bias' & 'bias_confidence' flags.
import os
from datetime import datetime
from policy_bias_engine import get_bias_map

# guards for scaling
MIN_FACTOR = float(os.getenv("POLICY_BIAS_MIN", "0.75"))
MAX_FACTOR = float(os.getenv("POLICY_BIAS_MAX", "1.25"))
APPLY_TO = os.getenv("POLICY_BIAS_APPLY_TO", "amount,notional_usd").split(",")  # fields to scale if present
MIN_CONF = float(os.getenv("POLICY_BIAS_MIN_CONF", "0.10"))  # require at least this confidence to apply

def apply_predictive_bias(intent: dict) -> dict:
    if not isinstance(intent, dict):
        return {}
    sym = str(intent.get("symbol") or intent.get("token") or "").upper()
    if not sym:
        return {}

    bias = get_bias_map() or {}
    if sym not in bias:
        return {}

    factor, conf = bias.get(sym, (1.0, 0.0))
    if conf < MIN_CONF:
        return {}

    patched = {}
    for field in APPLY_TO:
        k = field.strip()
        if not k: continue
        if k in intent:
            try:
                v = float(intent[k])
                nv = max(0.0, min(v * factor, v * MAX_FACTOR))
                nv = max(nv, v * MIN_FACTOR)
                patched[k] = round(nv, 8)
            except Exception:
                continue

    flags = [f"policy_bias:{factor:.3f}", f"bias_conf:{conf:.2f}"]
    return {"patched_intent": patched, "flags": flags, "factor": factor, "confidence": conf, "symbol": sym}

# convenience: one-shot preview
def preview_bias(symbol: str):
    bm = get_bias_map()
    sym = str(symbol or "").upper()
    return {sym: bm.get(sym)}
