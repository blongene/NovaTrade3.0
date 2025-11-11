
# --- Optional venue alias normalization ---
_KRAKEN_BASE_ALIASES = {"BTC": "XBT"}
_COINBASE_QUOTE_ALIASES = {"USDT": "USD"}  # enable only if your Coinbase executor requires USD

def _venue_symbol_remap(base: str, quote: str, venue: str):
    v = (venue or "").upper()
    if v == "KRAKEN":
        base = _KRAKEN_BASE_ALIASES.get(base, base)
    if v == "COINBASE":
        quote = _COINBASE_QUOTE_ALIASES.get(quote, quote)
    return base, quote


# router.py — Phase 7C + Phase 10 Predictive Bias (fixed symbol parsing)
from __future__ import annotations
import os, re, time
from typing import Dict, Any, Tuple, Optional

_PAIR_RE = re.compile(r"^([A-Z0-9]+)[-/]([A-Z0-9]+)$")  # require an explicit separator

# Venue/quote min notionals (US-dollar equivalent per quote)
_MIN_NOTIONAL = {
    "BINANCEUS": {"USDT": 10, "USDC": 10, "USD": 10},
    "COINBASE":  {"USDC":  1, "USD":  1},
    "KRAKEN":    {"USDT": 25, "USD": 10, "USDC": 10},
}

def _parse(symbol: str) -> Tuple[str, str]:
    s = symbol.upper().replace("_","/").replace(":","/").replace(".","/")
    s = s.replace("--","-").replace("//","/")
    m = _PAIR_RE.match(s)
    if not m:
        raise ValueError(f"bad symbol: {symbol}")
    return m.group(1), m.group(2)

def _normalize_for_venue(symbol: str, venue: str, prefer_quote: Optional[str]) -> Tuple[str, list]:
    """Return venue-friendly *internal* symbol 'BASE-QUOTE' and flags.
    - We *keep* a hyphen between base/quote for readability.
    - If prefer_quote provided and both are USD-family, swap quote.
    """
    base, quote = _parse(symbol)
    flags = []
    if prefer_quote and quote != prefer_quote:
        if quote in ("USD","USDT","USDC") and prefer_quote in ("USD","USDT","USDC"):
            quote = prefer_quote
            flags.append("prefer_quote")
    return f"{base}-{quote}", flags

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
    return ["BINANCEUS","COINBASE","KRAKEN"]

def _prefer_map(cfg: Dict[str,Any]) -> Dict[str,str]:
    pm = cfg.get("prefer_quotes") or {}
    return {str(k).upper(): str(v).upper() for k,v in pm.items() if v}

def _quote_of(sym_hyphen: str) -> str:
    return _parse(sym_hyphen)[1]

def _min_notional(venue: str, quote: str) -> float:
    return float(_MIN_NOTIONAL.get(venue, {}).get(quote, 0))

def _apply_predictive_bias_safe(intent: Dict[str,Any]):
    if os.getenv("ENABLE_PREDICTIVE_BIAS", "1").lower() not in ("1","true","yes","on"):
        return {}, []
    try:
        from predictive_policy_driver import apply_predictive_bias
        res = apply_predictive_bias(dict(intent)) or {}
        return (res.get("patched_intent") or {}, res.get("flags") or [])
    except Exception:
        return {}, []

def choose_venue(intent: Dict[str,Any], telemetry: Dict[str,Any], policy_cfg: Dict[str,Any]) -> Dict[str,Any]:
    desired_symbol = str(intent.get("symbol","")).upper()  # keep separator
    venue_hint = str(intent.get("venue","") or "").upper() or None
    price_usd = intent.get("price_usd")
    raw_amount = float(intent.get("amount", 0.0))

    flags = []
    patched: Dict[str,Any] = {}

    tel_age = _telemetry_age_sec(telemetry) or 0
    tel_max = _get(policy_cfg, "telemetry_max_age_sec", int(os.getenv("POLICY_TEL_MAX_AGE_SEC", "600")))
    if tel_max and tel_age > tel_max:
        return {"ok": False, "reason": f"telemetry stale ({int(tel_age)}s > {tel_max}s)", "flags": ["telemetry_stale"]}

    prefer = _prefer_map(policy_cfg)
    keepback = _get(policy_cfg, "keepback_usd", 5.0)
    min_reserve = _get(policy_cfg, "min_quote_reserve_usd", 10.0)
    canary_max = _get(policy_cfg, "canary_max_usd", 10.0)
    max_per = _get(policy_cfg, "max_per_coin_usd", 25.0)

    venues = [venue_hint] if venue_hint else _venue_order(policy_cfg)
    last_reason = "no venue considered"

    for v in venues:
        pv = prefer.get(v)
        sym_norm, sflags = _normalize_for_venue(desired_symbol, v, pv)
        flags.extend(sflags)
        quote = _quote_of(sym_norm)

        by_venue = (telemetry or {}).get("by_venue", {})
        v_bal = float(((by_venue.get(v) or {}).get(quote) or 0.0))

        usable = max(0.0, v_bal - keepback)
        if usable < min_reserve:
            last_reason = f"{v} below min reserve ({usable:.2f} < {min_reserve:.2f} {quote})"
            continue

        patched["venue"] = v
        patched["symbol"] = sym_norm  # keep hyphenated

        if price_usd:
            min_notional = _min_notional(v, quote)
            need_amount_for_min = (min_notional / float(price_usd)) if min_notional > 0 else 0.0

            amt = raw_amount
            if min_notional and (amt * float(price_usd) < min_notional):
                amt = need_amount_for_min
                flags.append("min_notional_bump")

            notional = amt * float(price_usd)
            if notional > canary_max:
                amt = canary_max / float(price_usd); flags.append("clamped_canary")
            if notional > max_per:
                amt = max_per / float(price_usd); flags.append("clamped_max_per_coin")

            spend = amt * float(price_usd)
            if spend > usable:
                last_reason = f"{v} insufficient {quote}: need {spend:.2f}, have {usable:.2f}"
                continue
            if min_notional and spend < min_notional:
                last_reason = f"{v} min notional {min_notional:.2f} {quote} not met (have {spend:.2f})"
                continue

            # Phase 10 bias
            try:
                bias_patch, bias_flags = _apply_predictive_bias_safe({
                    "symbol": sym_norm, "venue": v, "amount": amt, "price_usd": price_usd, "notional_usd": spend,
                })
                if bias_patch:
                    amt_biased = float(bias_patch.get("amount", amt))
                    spend_biased = amt_biased * float(price_usd)
                    if spend_biased > max_per:
                        amt_biased = max_per / float(price_usd); bias_flags.append("bias_clamped_max_per_coin")
                    if spend_biased > canary_max:
                        amt_biased = canary_max / float(price_usd); bias_flags.append("bias_clamped_canary")
                    spend_biased = amt_biased * float(price_usd)
                    if spend_biased > usable:
                        bias_flags.append("bias_rejected_insufficient_balance")
                    else:
                        if min_notional and spend_biased < min_notional:
                            amt_biased = need_amount_for_min
                            spend_biased = amt_biased * float(price_usd)
                            bias_flags.append("bias_bumped_min_notional")
                        amt = amt_biased; spend = spend_biased; flags.extend(bias_flags)
            except Exception:
                pass

            patched["amount"] = round(amt, 12)
            return {"ok": True, "reason": "ok", "patched_intent": patched, "flags": flags}

        else:
            flags.append("min_notional_unknown")
            try:
                bias_patch, bias_flags = _apply_predictive_bias_safe({
                    "symbol": sym_norm, "venue": v, "amount": raw_amount,
                })
                amt_out = float(bias_patch.get("amount", raw_amount))
                patched["amount"] = round(amt_out, 12); flags.extend(bias_flags or [])
            except Exception:
                patched["amount"] = raw_amount
            return {"ok": True, "reason": "ok_no_price", "patched_intent": patched, "flags": flags}

    return {"ok": False, "reason": last_reason or "no venue usable", "flags": flags}
