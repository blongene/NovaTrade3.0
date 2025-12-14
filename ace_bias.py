# ace_bias.py
# Phase 21.7 — ACE v1: feedback loop into policy (SOFT influence only)
# Uses existing env vars from NovaTrade3.0.txt
#
# - Mode "log_only": computes multipliers but returns original weights (no influence)
# - Mode "soft_weight": multiplies weights by bounded multipliers
# - Fail-open: any error returns original weights
#
# Expected Council_Performance columns (flexible casing):
#   Voice, Trades, Success_Rate, Error_Rate
#
# Note: This module does NOT mutate intents. It only adjusts weights.

from __future__ import annotations
import os, time
from typing import Any, Dict

try:
    from utils import sheets_get_records_cached
except Exception:
    sheets_get_records_cached = None  # fail-open

SHEET_URL = os.getenv("SHEET_URL", "")

# Existing env vars — do NOT rename
ACE_ENABLED = os.getenv("ACE_FEEDBACK_ENABLED", "0").lower() in {"1","true","yes","on"}
COUNCIL_ACE_ENABLE = os.getenv("COUNCIL_ACE_ENABLE", "0").lower() in {"1","true","yes","on"}
ACE_MODE = (os.getenv("ACE_FEEDBACK_MODE", "log_only") or "log_only").strip().lower()  # log_only|soft_weight

ACE_MIN = float(os.getenv("ACE_FEEDBACK_MIN", "0.90"))
ACE_MAX = float(os.getenv("ACE_FEEDBACK_MAX", "1.15"))
ACE_MAX_STEP = float(os.getenv("ACE_FEEDBACK_MAX_STEP", "0.02"))  # per refresh clamp
ACE_TTL = int(os.getenv("ACE_FEEDBACK_TTL_SEC", "1800"))

# Your env uses Council_* too; honor both TTLs safely (take min)
COUNCIL_ACE_WS = os.getenv("COUNCIL_ACE_WS", "Council_Performance")
COUNCIL_ACE_CACHE_SECONDS = int(os.getenv("COUNCIL_ACE_CACHE_SECONDS", "300"))

# Internal cache: multipliers (voice->mult), plus last applied for step limiting
_cache = {
    "ts": 0.0,
    "mults": {},       # current multipliers
    "last_mults": {},  # last returned multipliers (for max-step limiting)
}

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v in ("", None): return default
        return float(v)
    except Exception:
        return default

def _i(v: Any, default: int = 0) -> int:
    try:
        if v in ("", None): return default
        return int(float(v))
    except Exception:
        return default

def _ace_score(success_rate: float, error_rate: float) -> float:
    # SD45-ish: 0.7*success + 0.3*(1-error)
    s = _clamp(success_rate, 0.0, 1.0)
    e = _clamp(error_rate, 0.0, 1.0)
    return _clamp(0.7 * s + 0.3 * (1.0 - e), 0.0, 1.0)

def _raw_multiplier(score: float) -> float:
    # 0.95 + 0.20*score, then clamp to env bounds
    return _clamp(0.95 + 0.20 * score, ACE_MIN, ACE_MAX)

def _ttl_seconds() -> int:
    # honor both; pick the smaller to avoid stale ACE in fast-moving ops
    return max(30, min(ACE_TTL, COUNCIL_ACE_CACHE_SECONDS))

def _load_multipliers() -> Dict[str, float]:
    """
    Returns {voice_lower: multiplier}.
    Cached with TTL.
    """
    if not (ACE_ENABLED and COUNCIL_ACE_ENABLE and SHEET_URL and sheets_get_records_cached):
        return {}

    now = time.time()
    if now - _cache["ts"] < _ttl_seconds():
        return _cache["mults"]

    rows = sheets_get_records_cached(SHEET_URL, COUNCIL_ACE_WS) or []
    mults: Dict[str, float] = {}

    for r in rows:
        voice = (r.get("Voice") or r.get("voice") or "").strip()
        if not voice:
            continue

        trades = _i(r.get("Trades") or r.get("trades") or 0, 0)
        # Warmup rule: require at least a handful of outcomes.
        if trades < 10:
            continue

        success = _f(r.get("Success_Rate") or r.get("success_rate") or 0.0, 0.0)
        error = _f(r.get("Error_Rate") or r.get("error_rate") or 0.0, 0.0)

        score = _ace_score(success, error)
        mults[voice.lower()] = round(_raw_multiplier(score), 6)

    _cache["ts"] = now
    _cache["mults"] = mults
    return mults

def _step_limit(new_mults: Dict[str, float]) -> Dict[str, float]:
    """
    Limits multiplier movement per refresh using ACE_FEEDBACK_MAX_STEP.
    Prevents ACE oscillations and keeps it "soft".
    """
    last = _cache.get("last_mults") or {}
    out: Dict[str, float] = {}

    for k, v in new_mults.items():
        prev = float(last.get(k, v))
        lo = prev - ACE_MAX_STEP
        hi = prev + ACE_MAX_STEP
        out[k] = round(_clamp(float(v), lo, hi), 6)

    _cache["last_mults"] = dict(out)
    return out

def get_ace_debug_snapshot() -> Dict[str, Any]:
    """
    Optional: call this for logging/telemetry.
    """
    mults = _step_limit(_load_multipliers())
    return {
        "enabled": bool(ACE_ENABLED and COUNCIL_ACE_ENABLE),
        "mode": ACE_MODE,
        "ws": COUNCIL_ACE_WS,
        "ttl_s": _ttl_seconds(),
        "bounds": {"min": ACE_MIN, "max": ACE_MAX, "max_step": ACE_MAX_STEP},
        "multipliers": mults,
    }

def apply_ace_bias(council: Dict[str, float]) -> Dict[str, float]:
    """
    Main hook called by council_influence.py.
    Input:  {voice: weight}
    Output: adjusted weights if ACE_FEEDBACK_MODE=soft_weight; else original weights.
    """
    try:
        if not isinstance(council, dict) or not council:
            return council

        mults = _step_limit(_load_multipliers())
        if not mults:
            return council

        if ACE_MODE == "log_only":
            return council

        if ACE_MODE != "soft_weight":
            # Unknown mode -> fail-open (no bias)
            return council

        out: Dict[str, float] = {}
        for voice, w in council.items():
            vw = _f(w, 0.0)
            m = float(mults.get(str(voice).lower(), 1.0))
            out[voice] = round(vw * m, 6)
        return out

    except Exception:
        return council
