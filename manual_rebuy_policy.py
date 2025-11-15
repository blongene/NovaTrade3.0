# manual_rebuy_policy.py — Safe manual rebuy wrapper around PolicyEngine
from __future__ import annotations
import math
from typing import Dict, Any

from policy_engine import PolicyEngine

_EPS = 1e-9


def _to_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def evaluate_manual_rebuy(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    High-level policy wrapper for MANUAL_REBUY intents.

    Input `intent` is expected to look like:
      {
        "source": "manual_rebuy",
        "token": "BTC",
        "action": "BUY",
        "amount_usd": 25.0,
        "venue": "BINANCEUS",
        "quote": "USDT" | "USD" | "USDC" | "",
        "ts": 1700000000,
        ... (any other fields are passed through)
      }

    Returns a decision dict:

      {
        "ok": bool,
        "status": "APPROVED" | "CLIPPED" | "DENIED",
        "reason": str,
        "original_amount_usd": float,
        "patched": dict,          # patched intent from PolicyEngine
      }

    Notes:
      * We intentionally delegate all sizing / venue caps / notional limits
        to PolicyEngine, so this stays as a thin, well-typed facade.
      * Classification into APPROVED vs CLIPPED vs DENIED is based purely on:
          - ok flag from PolicyEngine
          - comparison of requested vs patched notional_usd / amount_usd
    """
    pe = PolicyEngine()

    # normalize inputs
    orig_amt_usd = _to_float(
        intent.get("amount_usd") or intent.get("notional_usd") or 0.0,
        0.0,
    )

    # Let the engine do the heavy lifting.
    # For now we don't pass any extra asset_state context; venue budgets / telemetry
    # can plug in here later without changing nova_trigger.
    ok, reason, patched = pe.validate(dict(intent), asset_state={})  # type: ignore[arg-type]

    patched_amt = _to_float(
        (patched or {}).get("amount_usd")
        or (patched or {}).get("notional_usd")
        or orig_amt_usd,
        orig_amt_usd,
    )

    # Decide on status label
    if not ok:
        status = "DENIED"
    else:
        # treat small float jitter as 'same'
        if orig_amt_usd <= 0:
            status = "APPROVED"
        else:
            rel_diff = abs(patched_amt - orig_amt_usd) / max(orig_amt_usd, _EPS)
            if rel_diff > 0.01 and patched_amt < orig_amt_usd:  # >1% reduction => clipped
                status = "CLIPPED"
            else:
                status = "APPROVED"

    return {
        "ok": bool(ok),
        "status": status,
        "reason": reason or "",
        "original_amount_usd": orig_amt_usd,
        "patched": patched or {},
    }
