# ace_feedback.py
# Phase 21.7 — ACE v1 (Adaptive Council Equilibrium)
# SOFT INFLUENCE ONLY — bounded multipliers

from __future__ import annotations
import os, time
from typing import Dict
from utils import sheets_get_records_cached

SHEET_URL = os.getenv("SHEET_URL")
PERF_WS = os.getenv("COUNCIL_PERFORMANCE_WS", "Council_Performance")

ENABLED = os.getenv("ACE_FEEDBACK_ENABLED", "0").lower() in {"1","true","yes"}
WARMUP_MIN = int(os.getenv("ACE_WARMUP_TRADES", "20"))
TTL_SEC = int(os.getenv("ACE_CACHE_TTL_SEC", "900"))

MIN_M = float(os.getenv("ACE_MIN_MULT", "0.90"))
MAX_M = float(os.getenv("ACE_MAX_MULT", "1.15"))

_cache = {"ts": 0, "data": {}}

def _clamp(x: float) -> float:
    return max(MIN_M, min(MAX_M, x))

def _compute(score: float) -> float:
    base = 0.95 + 0.20 * score
    return _clamp(base)

def get_ace_multipliers() -> Dict[str, float]:
    if not ENABLED or not SHEET_URL:
        return {}

    now = time.time()
    if now - _cache["ts"] < TTL_SEC:
        return _cache["data"]

    rows = sheets_get_records_cached(SHEET_URL, PERF_WS)
    if not rows:
        return {}

    mults: Dict[str, float] = {}

    for r in rows:
        voice = r.get("Voice")
        try:
            trades = int(r.get("Trades", 0))
            success = float(r.get("Success_Rate", 0))
            error = float(r.get("Error_Rate", 0))
        except Exception:
            continue

        if not voice or trades < WARMUP_MIN:
            continue

        ace_score = max(0.0, min(1.0, 0.7 * success + 0.3 * (1 - error)))
        mults[voice] = round(_compute(ace_score), 4)

    _cache["ts"] = now
    _cache["data"] = mults
    return mults
