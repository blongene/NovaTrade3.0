# council_analytics.py
# Utilities for Phase 21.6/21.7 metrics

from __future__ import annotations
from typing import Dict, Tuple

def majority_voice(weights: Dict[str, float]) -> str:
    if not isinstance(weights, dict) or not weights:
        return ""
    best_k = ""
    best_v = None
    for k, v in weights.items():
        try:
            fv = float(v or 0.0)
        except Exception:
            fv = 0.0
        if best_v is None or fv > best_v:
            best_v = fv
            best_k = str(k)
    return best_k

def disagreement_index(weights: Dict[str, float]) -> float:
    """
    SD45 definition: 1 - max(council_weights)
    """
    if not isinstance(weights, dict) or not weights:
        return 0.0
    mx = 0.0
    for v in weights.values():
        try:
            fv = float(v or 0.0)
        except Exception:
            fv = 0.0
        if fv > mx:
            mx = fv
    di = 1.0 - mx
    # keep in [0..1]
    if di < 0.0: di = 0.0
    if di > 1.0: di = 1.0
    return round(di, 6)

def split_tag(di: float) -> str:
    # simple bands; tweak later without breaking schema
    if di >= 0.55:
        return "HIGH"
    if di >= 0.35:
        return "MED"
    return "LOW"
