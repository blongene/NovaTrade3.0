# router.py — Phase 7B venue chooser + pre-sizing
from __future__ import annotations
from typing import Dict, Any, Optional, Tuple

PAIR_SEP_VENUES = {"COINBASE", "COINBASEADV", "CBADV"}

def _split_symbol(symbol: str) -> Tuple[str, str]:
    s = (symbol or "").upper().replace(":", "").replace(".", "")
    if "-" in s: 
        a,b = s.split("-",1); return a,b
    # naive fallback -> assume QUOTE is last 3/4
    for k in (4, 3):
        if len(s) > k:
            return s[:-k], s[-k:]
    return s, "USD"

def _join_symbol(base: str, quote: str, venue: str) -> str:
    return f"{base}-{quote}" if venue.upper() in PAIR_SEP_VENUES else f"{base}{quote}"

def choose_venue(intent: Dict[str,Any], telemetry: Dict[str,Any], policy_cfg: Dict[str,Any]) -> Dict[str,Any]:
    """Returns {ok, reason, patched_intent, flags}"""
    base, quote = _split_symbol(intent.get("symbol",""))
    requested_venue = str(intent.get("venue","")).upper()
    venue_order = policy_cfg.get("venue_order") or ["BINANCEUS","COINBASE","KRAKEN"]
    # If user specified a venue, keep it first
    order = [requested_venue] + [v for v in venue_order if v != requested_venue]

    by_venue = (telemetry or {}).get("by_venue") or {}
    keepback = float(policy_cfg.get("keepback_usd", 5) or 0)
    canary   = float(policy_cfg.get("canary_max_usd", 10) or 0)

    best: Optional[Tuple[str, float]] = None
    for v in order:
        bal = (by_venue.get(v.upper()) or {}).get(quote.upper())
        if bal is None: 
            continue
        usable = max(0.0, float(bal) - keepback)
        usable = min(usable, canary) if canary > 0 else usable
        if usable <= 0.0:
            continue
        if (best is None) or (usable > best[1]):
            best = (v, usable)

    if not best:
        return {"ok": False, "reason": "no venue with usable quote", "patched_intent": {}, "flags": ["no_venue_usable"]}

    venue, usable = best
    patched = dict(intent)
    patched["venue"] = venue
    patched["symbol"] = _join_symbol(base, quote, venue)
    # amount sizing left to policy (we only pick venue and normalize symbol)
    return {"ok": True, "reason": "routed", "patched_intent": {"venue": venue, "symbol": patched["symbol"]}, "flags": ["routed"]}
