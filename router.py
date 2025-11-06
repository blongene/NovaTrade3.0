# router.py — Phase 7B venue chooser + pre-sizing (prefer_quote-aware)

from __future__ import annotations
from typing import Dict, Any, Optional, Tuple

PAIR_SEP_VENUES = {"COINBASE", "COINBASEADV", "CBADV"}

def _split_symbol(symbol: str) -> Tuple[str, str]:
    s = (symbol or "").upper().replace(":", "").replace(".", "")
    if "-" in s:
        a,b = s.split("-",1); return a,b
    if "/" in s:
        a,b = s.split("/",1); return a,b
    # common 4-char quotes
    for q in ("USDT","USDC","BUSD","TUSD"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    # common 3-char quotes
    for q in ("USD","EUR","BTC","ETH"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    # fallback
    return s, "USD"

def _join_symbol(base: str, quote: str, venue: str) -> str:
    return f"{base}-{quote}" if venue.upper() in PAIR_SEP_VENUES else f"{base}{quote}"

def choose_venue(intent: Dict[str,Any], telemetry: Dict[str,Any], policy_cfg: Dict[str,Any]) -> Dict[str,Any]:
    """
    Picks a venue using telemetry and policy preferences.
    IMPORTANT: Uses prefer_quotes DURING selection so BTC-USD for KRAKEN
    gets evaluated against USDT balance.
    """
    base, in_quote = _split_symbol(intent.get("symbol",""))
    requested_venue = str(intent.get("venue","")).upper()

    prefer_quotes = (policy_cfg or {}).get("prefer_quotes") or {}
    venue_order = (policy_cfg or {}).get("venue_order") or ["BINANCEUS","COINBASE","KRAKEN"]
    # keep requested venue first if provided
    order = [requested_venue] + [v for v in venue_order if v != requested_venue] if requested_venue else venue_order

    by_venue = (telemetry or {}).get("by_venue") or {}
    keepback = float((policy_cfg or {}).get("keepback_usd", 5) or 0)
    canary   = float((policy_cfg or {}).get("canary_max_usd", 10) or 0)

    best: Optional[Tuple[str, float, str]] = None  # (venue, usable, chosen_quote)

    for v in order:
        balmap = (by_venue.get(v.upper()) or {})
        if not balmap:
            continue

        # try preferred quote first, then original input quote
        pref_q = prefer_quotes.get(v.upper(), in_quote.upper())
        for q in (pref_q, in_quote.upper()):
            bal = balmap.get(q)
            if bal is None:
                continue
            usable = max(0.0, float(bal) - keepback)
            if canary > 0:
                usable = min(usable, canary)
            if usable <= 0.0:
                continue
            if (best is None) or (usable > best[1]):
                best = (v, usable, q)

    if not best:
        return {"ok": False, "reason": "no venue with usable quote", "patched_intent": {}, "flags": ["no_venue_usable"]}

    venue, usable, out_quote = best
    patched = {"venue": venue, "symbol": _join_symbol(base, out_quote, venue)}
    return {"ok": True, "reason": "routed", "patched_intent": patched, "flags": ["routed"]}
