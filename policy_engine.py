# policy_engine.py — Phase 7A (context-aware reserves, notional caps, symbol normalization)
from __future__ import annotations
import os, json, re
from typing import Any, Dict, Optional, Tuple

try:
    from policy_logger import log_decision as _policy_log
except Exception:
    def _policy_log(*args, **kwargs):
        return

# ---------- Defaults ----------
_DEFAULT = {
    "policy": {
        # Pair/venue rules
        "prefer_quotes": {"BINANCEUS":"USDT","COINBASE":"USDC","KRAKEN":"USDT"},
        "blocked_symbols": ["BARK","BONK"],

        # Risk & sizing
        "max_per_coin_usd": 25,            # hard cap per intent
        "min_quote_reserve_usd": 25,       # ignore if quote < this
        "keepback_usd": 5,                 # never breach this reserve
        "canary_max_usd": 10,              # first sizing cap for safety
        "on_short_quote": "resize",        # resize | deny

        # Price/telemetry expectations
        "allow_price_unknown": False,      # deny if no price and sizing needed

        # Cooldowns (stub; computed elsewhere)
        "cool_off_minutes_after_trade": 30
    }
}

# ---------- YAML loader (supports 'policy:' root or top-level risk/execution keys) ----------
def _load_yaml(path: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    try:
        import yaml  # type: ignore
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if isinstance(raw, dict):
            if "policy" in raw and isinstance(raw["policy"], dict):
                data = {"policy": {**_DEFAULT["policy"], **raw["policy"]}}
            else:
                # merge any known keys directly (risk/execution sections)
                merged = dict(_DEFAULT["policy"])
                for k in ("prefer_quotes","blocked_symbols","max_per_coin_usd",
                          "min_quote_reserve_usd","keepback_usd","canary_max_usd",
                          "on_short_quote","allow_price_unknown",
                          "cool_off_minutes_after_trade"):
                    if k in raw: merged[k] = raw[k]
                # nested sections
                for sec in ("risk","execution","caps","cooldowns"):
                    if isinstance(raw.get(sec), dict):
                        merged.update(raw[sec])
                data = {"policy": merged}
    except Exception:
        data = {}

    if not data:
        # last resort: defaults
        data = _DEFAULT
    return data

# ---------- Symbol helpers ----------
_PAIR_RE = re.compile(r"^([A-Z0-9]+)[-/]?([A-Z0-9]+)$")

def _split_symbol(symbol: str, venue: str) -> Tuple[str, str]:
    s = (symbol or "").upper().replace(":", "").replace(".", "")
    m = _PAIR_RE.match(s)
    if m:
        return m.group(1), m.group(2)
    if "-" in s:
        a, b = s.split("-", 1)
        return a, b
    return s, "USD"

def _join_symbol(base: str, quote: str, venue: str) -> str:
    v = (venue or "").upper()
    if v in ("COINBASE","COINBASEADV","CBADV"):
        return f"{base}-{quote}"
    return f"{base}{quote}"

# ---------- Utility ----------
def _float(x: Any, default: Optional[float]=None) -> Optional[float]:
    try: return float(x)
    except Exception: return default

def _get_quote_reserve_usd(context: Optional[dict], venue: str, quote: str) -> Optional[float]:
    if not context: return None
    tel = context.get("telemetry") or {}
    balances = (tel.get("by_venue") or {}).get(venue.upper()) or {}
    # map quote token directly; if USDC/USDT present, we approximate USD value as 1:1
    if quote in balances:
        return _float(balances.get(quote), 0.0)
    # fallback to flat (already summed)
    flat = tel.get("flat") or {}
    if quote in flat:
        return _float(flat.get(quote), 0.0)
    return None

def _cap_notional(cfg: dict, notional: Optional[float]) -> Tuple[Optional[float], list]:
    flags = []
    cap = _float(cfg.get("max_per_coin_usd"))
    if cap and notional is not None and notional > cap:
        flags.append("clamped")
        return cap, flags
    return notional, flags

# ---------- Engine ----------
class Engine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or _DEFAULT["policy"]

    def evaluate_intent(self, intent: Dict[str, Any], context: Optional[dict]=None) -> Dict[str, Any]:
        cfg = self.cfg
        venue = (intent.get("venue") or "").upper()
        symbol = (intent.get("symbol") or "").upper()
        side   = (intent.get("side") or "").lower()
        amount = _float(intent.get("amount"), 0.0) or 0.0

        # Blocks
        base, quote = _split_symbol(symbol, venue)
        if base in [s.upper() for s in (cfg.get("blocked_symbols") or [])]:
            decision = {"ok": False, "reason": f"blocked symbol {base}", "patched_intent": {}, "flags": ["blocked"]}
            _policy_log(decision=decision, intent=intent, when=None)
            return decision

        # Prefer quote per venue (e.g., COINBASE=USDC, KRAKEN=USDT)
        patched: Dict[str, Any] = {}
        flags = []
        prefer_quote = (cfg.get("prefer_quotes") or {}).get(venue)
        if prefer_quote and quote != prefer_quote:
            quote = prefer_quote
            patched["symbol"] = _join_symbol(base, quote, venue)
            flags.append("prefer_quote")

        price = _float(intent.get("price_usd"))
        notional = _float(intent.get("notional_usd"))
        if notional is None and price is not None:
            notional = price * amount
        elif notional is None and price is None:
            flags.append("notional_unknown")

        # Cap overall notional
        notional, clamp_flags = _cap_notional(cfg, notional)
        flags += clamp_flags
        if "clamped" in clamp_flags and price:
            # if we clamped notional and we have a price, adjust amount
            patched["amount"] = round(notional / price, 8)

        # Quote reserve logic (context-aware)
        min_reserve = _float(cfg.get("min_quote_reserve_usd"))
        keepback = _float(cfg.get("keepback_usd"), 0.0) or 0.0
        on_short = (cfg.get("on_short_quote") or "resize").lower()

        quote_reserve = intent.get("quote_reserve_usd")
        if quote_reserve is None:
            # pull from telemetry context if present
            quote_reserve = _get_quote_reserve_usd(context, venue, quote)
            if quote_reserve is None:
                flags.append("reserve_unknown")

        # Buy-side: ensure sufficient quote; optionally auto-resize
        if side == "buy":
            # require price to compute amount sizing; optionally deny if unknown
            if price is None and not cfg.get("allow_price_unknown", False):
                decision = {"ok": False, "reason": "price unknown; sizing requires price", "patched_intent": patched, "flags": flags + ["price_unknown"]}
                _policy_log(decision=decision, intent=intent, when=None)
                return decision

            if isinstance(quote_reserve, (int, float)):
                # available quote after keepback
                usable = max(0.0, float(quote_reserve) - (keepback or 0.0))
                if min_reserve and float(quote_reserve) < float(min_reserve):
                    # below minimum reserve → either deny or resize to zero
                    if on_short == "resize":
                        # effectively deny by resizing to zero
                        patched["amount"] = 0.0
                        decision = {"ok": False, "reason": f"quote below min reserve (${quote_reserve:.2f} < ${min_reserve:.2f})", "patched_intent": patched, "flags": flags + ["below_min_reserve"]}
                        _policy_log(decision=decision, intent=intent, when=None)
                        return decision
                    else:
                        decision = {"ok": False, "reason": f"quote below min reserve (${quote_reserve:.2f} < ${min_reserve:.2f})", "patched_intent": patched, "flags": flags + ["below_min_reserve"]}
                        _policy_log(decision=decision, intent=intent, when=None)
                        return decision

                # apply canary cap
                canary_cap = _float(cfg.get("canary_max_usd"))
                if canary_cap is not None:
                    usable = min(usable, float(canary_cap))

                if price is not None:
                    target_amount = round(max(0.0, usable) / float(price), 8)
                    if amount > target_amount:
                        if on_short == "resize" and target_amount > 0.0:
                            patched["amount"] = target_amount
                            flags.append("auto_resized")
                        else:
                            decision = {"ok": False, "reason": f"insufficient quote: have ${quote_reserve:.2f}, usable ${usable:.2f}", "patched_intent": patched, "flags": flags + ["insufficient_quote"]}
                            _policy_log(decision=decision, intent=intent, when=None)
                            return decision
                else:
                    flags.append("price_unknown")

        # Final decision
        decision = {"ok": True, "reason": "ok", "patched_intent": patched, "flags": flags}
        _policy_log(decision=decision, intent=intent, when=None)
        return decision

# --------- Public API expected by Bus ----------
def load_policy(path: str):
    cfg = (_load_yaml(path) or {}).get("policy") or _DEFAULT["policy"]
    return Engine(cfg)

# Back-compat wrappers the Bus might call
_engine_singleton: Optional[Engine] = None

def evaluate_intent(intent: Dict[str, Any], context: Optional[dict]=None) -> Dict[str, Any]:
    global _engine_singleton
    if _engine_singleton is None:
        cfg = (_load_yaml(os.getenv("POLICY_PATH") or "policy.yaml") or {}).get("policy") or _DEFAULT["policy"]
        _engine_singleton = Engine(cfg)
    return _engine_singleton.evaluate_intent(intent, context=context)

# Legacy fallback: .evaluate(intent) -> dict
def evaluate(intent: Dict[str, Any]) -> Dict[str, Any]:
    return evaluate_intent(intent, context=None)
