# router.py — Phase 7C (min-notional, cooldown-aware hints, telemetry freshness)
from __future__ import annotations
import os, re, time
from typing import Dict, Any, Tuple, Optional

_PAIR_RE = re.compile(r"^([A-Z0-9]+)[-/]?([A-Z0-9]+)$")

# Venue/quote min notionals (US-dollar equivalent per quote)
_MIN_NOTIONAL = {
    "BINANCEUS": {"USDT": 10, "USDC": 10, "USD": 10},
    "COINBASE":  {"USDC":  1, "USD":  1},    # spot allows tiny sizes; policy still clamps
    "KRAKEN":    {"USDT": 25, "USD": 10, "USDC": 10},
}

def _parse(symbol: str) -> Tuple[str, str]:
    m = _PAIR_RE.match(symbol.replace("PERP",""))
    if not m:
        raise ValueError(f"bad symbol: {symbol}")
    return m.group(1), m.group(2)

def _normalize_for_venue(symbol: str, venue: str, prefer_quote: Optional[str]) -> Tuple[str, list]:
    """Return venue-friendly symbol and flags. Handles USD→USDT/USDC promotion."""
    base, quote = _parse(symbol.upper().replace("_","").replace(":","").replace(".",""))
    flags = []
    if prefer_quote and quote != prefer_quote:
        # USD→USDT/USDC or USDC↔USDT
        if quote in ("USD","USDT","USDC") and prefer_quote in ("USD","USDT","USDC"):
            quote = prefer_quote
            flags.append("prefer_quote")
        # else: leave as-is
    return f"{base}{quote}", flags

def _get(cfg: Dict[str, Any], key: str, default):
    v = cfg.get(key, default)
    try:
        return type(default)(v)
    except Exception:
        return default

def _telemetry_age_sec(telemetry: Dict[str, Any]) -> Optional[float]:
    ts = telemetry.get("ts")
    if not ts: return None
    return max(0.0, time.time() - float(ts))

def _venue_order(cfg: Dict[str,Any]) -> list:
    vo = cfg.get("venue_order")
    if isinstance(vo, list) and vo: return [str(v).upper() for v in vo]
    # fallback deterministic order
    return ["BINANCEUS","COINBASE","KRAKEN"]

def _prefer_map(cfg: Dict[str,Any]) -> Dict[str,str]:
    pm = cfg.get("prefer_quotes") or {}
    return {str(k).upper(): str(v).upper() for k,v in pm.items() if v}

def _quote_of(sym: str) -> str:
    return _parse(sym)[1]

def _min_notional(venue: str, quote: str) -> float:
    return float(_MIN_NOTIONAL.get(venue, {}).get(quote, 0))

def choose_venue(intent: Dict[str,Any], telemetry: Dict[str,Any], policy_cfg: Dict[str,Any]) -> Dict[str,Any]:
    """
    Decides best venue, normalizes symbol, enforces min-notional and basic balance guards.
    Returns: {ok, reason, patched_intent, flags}
    """
    # --- inputs
    desired_symbol = str(intent.get("symbol","")).upper().replace("-","")
    venue_hint = str(intent.get("venue","") or "").upper() or None
    price_usd = intent.get("price_usd")  # may be None (then we can't compute notional)
    raw_amount = float(intent.get("amount", 0.0))

    flags = []
    patched: Dict[str,Any] = {}

    # --- telemetry freshness (hard stop)
    tel_age = _telemetry_age_sec(telemetry) or 0
    tel_max = _get(policy_cfg, "telemetry_max_age_sec", int(os.getenv("POLICY_TEL_MAX_AGE_SEC", "600")))
    if tel_max and tel_age > tel_max:
        return {"ok": False, "reason": f"telemetry stale ({int(tel_age)}s > {tel_max}s)", "flags": ["telemetry_stale"]}

    # --- policy knobs
    prefer = _prefer_map(policy_cfg)
    keepback = _get(policy_cfg, "keepback_usd", 5.0)
    min_reserve = _get(policy_cfg, "min_quote_reserve_usd", 10.0)
    canary_max = _get(policy_cfg, "canary_max_usd", 10.0)
    max_per = _get(policy_cfg, "max_per_coin_usd", 25.0)

    venues = [venue_hint] if venue_hint else _venue_order(policy_cfg)
    last_reason = "no venue considered"

    for v in venues:
        pv = prefer.get(v)  # preferred quote on this venue
        sym_norm, sflags = _normalize_for_venue(desired_symbol, v, pv)
        flags.extend(sflags)
        quote = _quote_of(sym_norm)

        # balance for this quote
        by_venue = (telemetry or {}).get("by_venue", {})
        v_bal = float(((by_venue.get(v) or {}).get(quote) or 0.0))

        # guard: keepback + minimum reserve
        usable = max(0.0, v_bal - keepback)
        if usable < min_reserve:
            last_reason = f"{v} below min reserve ({usable:.2f} < {min_reserve:.2f} {quote})"
            continue

        patched["venue"] = v
        patched["symbol"] = f"{_parse(sym_norm)[0]}-{quote}"  # keep hyphen for display/API payloads too

        # If we can size by price, enforce min-notional + canary/max caps
        if price_usd:
            min_notional = _min_notional(v, quote)
            target_notional = max(min_notional, min(canary_max, max_per))
            # try to meet at least min_notional first
            need_amount_for_min = (min_notional / float(price_usd)) if min_notional > 0 else 0.0

            # start from operator amount, bump if needed to min_notional
            amt = raw_amount
            if min_notional and (amt * float(price_usd) < min_notional):
                amt = need_amount_for_min
                flags.append("min_notional_bump")

            # and clamp to canary / max_per by notional
            notional = amt * float(price_usd)
            if notional > canary_max:
                amt = canary_max / float(price_usd)
                flags.append("clamped_canary")
            if notional > max_per:
                amt = max_per / float(price_usd)
                flags.append("clamped_max_per_coin")

            # check we still have quote usability
            spend = amt * float(price_usd)
            if spend > usable:
                last_reason = f"{v} insufficient {quote}: need {spend:.2f}, have {usable:.2f}"
                continue

            if min_notional and spend < min_notional:
                last_reason = f"{v} min notional {min_notional:.2f} {quote} not met (have {spend:.2f})"
                continue

            patched["amount"] = round(amt, 12)  # safe rounding
            return {"ok": True, "reason": "ok", "patched_intent": patched, "flags": flags}

        else:
            # No price — we can't enforce notional; route but mark advisory flag.
            flags.append("min_notional_unknown")
            patched["amount"] = raw_amount
            return {"ok": True, "reason": "ok_no_price", "patched_intent": patched, "flags": flags}

    return {"ok": False, "reason": last_reason or "no venue usable", "flags": flags}
