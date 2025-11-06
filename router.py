# router.py — Phase 7B venue chooser + pre-sizing (prefer_quote-aware)
# Drop-in ready

from __future__ import annotations
from typing import Dict, Any, Optional, Tuple

# Venues that expect a hyphen separator (others use concatenated pairs)
PAIR_SEP_VENUES = {"COINBASE", "COINBASEADV", "CBADV"}

def _split_symbol(symbol: str) -> Tuple[str, str]:
    """
    Robustly split a symbol into base/quote. Accepts 'BTC-USD', 'BTC/USD', 'BTCUSD', 'BTCUSDT', etc.
    If we can't infer the quote, default to USD (policy may patch later).
    """
    s = (symbol or "").upper().replace(":", "").replace(".", "")
    if "-" in s:
        a, b = s.split("-", 1)
        return a, b
    if "/" in s:
        a, b = s.split("/", 1)
        return a, b

    # Heuristics for common quotes (longer first)
    for q in ("USDT", "USDC", "BUSD", "TUSD", "DAI"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    for q in ("USD", "EUR", "BTC", "ETH"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q

    return s, "USD"

def _join_symbol(base: str, quote: str, venue: str) -> str:
    return f"{base}-{quote}" if venue.upper() in PAIR_SEP_VENUES else f"{base}{quote}"

def _get_policy(cfg: Dict[str, Any]) -> Dict[str, Any]:
    # Defaults mirror policy_engine defaults; cfg is already the live policy.cfg dict from the Bus
    return {
        "prefer_quotes": cfg.get("prefer_quotes", {}),
        "venue_order":  cfg.get("venue_order", ["BINANCEUS", "COINBASE", "KRAKEN"]),
        "keepback_usd": float(cfg.get("keepback_usd", 5) or 0.0),
        "canary_max_usd": float(cfg.get("canary_max_usd", 10) or 0.0),
        "min_quote_reserve_usd": float(cfg.get("min_quote_reserve_usd", 10) or 0.0),
    }

def choose_venue(intent: Dict[str, Any],
                 telemetry: Dict[str, Any],
                 policy_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decide the best venue for an intent using telemetry balances and policy preferences.

    Behavior:
      • Honors prefer_quotes per-venue (e.g., KRAKEN→USDT, COINBASE→USDC).
      • Considers keepback_usd + canary_max_usd when computing "usable" quote.
      • If a venue is explicitly requested, it is considered first but may be skipped if unusable.
      • Returns patched 'venue' and normalized 'symbol'.
      • If price_usd & amount are provided and notional exceeds usable, we pre-clamp amount (flag 'pre_clamped').
    """
    p = _get_policy(policy_cfg or {})
    prefer_quotes = p["prefer_quotes"]
    venue_order = p["venue_order"]
    keepback = p["keepback_usd"]
    canary   = p["canary_max_usd"]
    min_res  = p["min_quote_reserve_usd"]

    by_venue = (telemetry or {}).get("by_venue") or {}
    requested_venue = str(intent.get("venue", "") or "").upper()
    base, in_quote = _split_symbol(str(intent.get("symbol", "")))

    # Build search order: requested venue first (if any), then the policy list (deduped)
    ordered = []
    if requested_venue:
        ordered.append(requested_venue)
    for v in venue_order:
        if v != requested_venue:
            ordered.append(v)

    best: Optional[Tuple[str, float, str]] = None  # (venue, usable_usd, chosen_quote)

    for v in ordered:
        bmap = (by_venue.get(v) or {})
        if not bmap:
            continue

        # Try preferred quote for that venue first; fallback to the incoming quote
        pref_q = prefer_quotes.get(v, in_quote)
        for q in (pref_q, in_quote):
            if q not in bmap:
                continue
            raw = float(bmap.get(q) or 0.0)
            usable = max(0.0, raw - keepback)
            if canary > 0:
                usable = min(usable, canary)

            if usable <= 0.0 or raw < min_res:
                continue

            # pick venue with the highest usable
            if best is None or usable > best[1]:
                best = (v, usable, q)

    if not best:
        return {
            "ok": False,
            "reason": "no venue with usable quote",
            "patched_intent": {},
            "flags": ["no_venue_usable"],
        }

    venue, usable_usd, out_quote = best
    patched = {
        "venue": venue,
        "symbol": _join_symbol(base, out_quote, venue),
    }
    flags = ["routed"]

    # Optional pre-clamp of amount if price_usd is present
    amt = intent.get("amount")
    price = intent.get("price_usd")
    try:
        if amt is not None and price is not None:
            amt_f = float(amt)
            price_f = float(price)
            if amt_f > 0 and price_f > 0:
                notional = amt_f * price_f
                if notional > usable_usd:
                    new_amt = usable_usd / price_f
                    # Guard against numerical dust; policy may still adjust
                    if new_amt <= 0:
                        return {
                            "ok": False,
                            "reason": "no usable notional after clamp",
                            "patched_intent": {},
                            "flags": ["no_venue_usable"],
                        }
                    patched["amount"] = new_amt
                    flags.append("pre_clamped")
    except Exception:
        # Be conservative: let policy handle edge cases
        pass

    return {"ok": True, "reason": "routed", "patched_intent": patched, "flags": flags}
