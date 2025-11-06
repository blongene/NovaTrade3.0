# policy_engine.py — Phase 7A (context-aware reserves, notional caps, symbol normalization)
from __future__ import annotations
import os, json, re
from typing import Any, Dict, Optional, Tuple

# Optional structured policy logger (degraded if unavailable)
try:
    from policy_logger import log_decision as _policy_log
except Exception:
    def _policy_log(*args, **kwargs):
        return

# --------------------------- Helpers ---------------------------

def _abs_path(p: str) -> str:
    if not p:
        return ""
    return p if os.path.isabs(p) else os.path.abspath(p)

def _merge(dst: dict, src: dict) -> dict:
    """shallow merge; src wins"""
    out = dict(dst or {})
    for k, v in (src or {}).items():
        out[k] = v
    return out

def _env_override_map() -> dict:
    """
    ENV overrides applied LAST (after YAML).
    Supported:
      POLICY_MAX_PER_COIN_USD
      POLICY_MIN_QUOTE_RESERVE_USD
      POLICY_KEEPBACK_USD
      POLICY_CANARY_MAX_USD
      POLICY_ALLOW_PRICE_UNKNOWN   (true/false)
      POLICY_PREFER_QUOTES_JSON    (e.g. {"BINANCEUS":"USDT","COINBASE":"USDC","KRAKEN":"USDT"})
    """
    m = {}
    def _f(name, key, cast=float):
        val = os.getenv(name)
        if val is not None:
            try:
                m[key] = cast(val)
            except Exception:
                m[key] = val  # last resort
    _f("POLICY_MAX_PER_COIN_USD", "max_per_coin_usd")
    _f("POLICY_MIN_QUOTE_RESERVE_USD", "min_quote_reserve_usd")
    _f("POLICY_KEEPBACK_USD", "keepback_usd")
    _f("POLICY_CANARY_MAX_USD", "canary_max_usd")
    _f("POLICY_ALLOW_PRICE_UNKNOWN", "allow_price_unknown",
       cast=lambda x: str(x).lower() in ("1","true","yes","on"))
    qmap = os.getenv("POLICY_PREFER_QUOTES_JSON")
    if qmap:
        try: m["prefer_quotes"] = json.loads(qmap)
        except Exception: pass
    return m

# --------------------------- Defaults ---------------------------

_DEFAULT = {
    "policy": {
        # Pair/venue rules
        "prefer_quotes": {"BINANCEUS":"USDT","COINBASE":"USDC","KRAKEN":"USDT"},
        "blocked_symbols": ["BARK","BONK"],

        # Risk & sizing
        "max_per_coin_usd": 25,      # hard cap per intent
        "min_quote_reserve_usd": 25, # deny/resize if quote below this floor
        "keepback_usd": 5,           # never breach this reserve
        "canary_max_usd": 10,        # first sizing cap for safety
        "on_short_quote": "resize",  # resize | deny

        # Price/telemetry expectations
        "allow_price_unknown": False,     # deny if no price and sizing needed

        # Cooldowns (stub; computed elsewhere)
        "cool_off_minutes_after_trade": 30,

        # Router preference order (if used upstream)
        "venue_order": ["BINANCEUS","COINBASE","KRAKEN"]
    }
}

# --------------------------- YAML loader ---------------------------

def _load_yaml(path: str) -> Dict[str, Any]:
    """
    Reads YAML at `path` (or defaults), merges onto _DEFAULT['policy'],
    then applies ENV overrides. Returns {"policy": <dict>, "_source": {...}}
    """
    try:
        import yaml  # type: ignore
    except Exception:
        yaml = None

    cfg = dict(_DEFAULT["policy"])
    apath = _abs_path(path or "policy.yaml")
    source: Dict[str, Any] = {"yaml": apath, "env_overrides": False, "defaults": False}

    # YAML (best effort)
    if yaml:
        try:
            with open(apath, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f) or {}
            if isinstance(raw, dict):
                node = raw.get("policy", raw)
                if isinstance(node, dict):
                    # Merge known keys / sections shallowly
                    merged = dict(cfg)
                    # shallow merge for a safe set of known keys
                    for k in (
                        "prefer_quotes", "blocked_symbols",
                        "max_per_coin_usd", "min_quote_reserve_usd",
                        "keepback_usd", "canary_max_usd", "on_short_quote",
                        "allow_price_unknown", "cool_off_minutes_after_trade",
                        "venue_order", "min_liquidity_usd",
                    ):
                        if k in node:
                            merged[k] = node[k]
                    # optional nested sections support
                    for sec in ("risk","execution","caps","cooldowns"):
                        if isinstance(node.get(sec), dict):
                            merged = _merge(merged, node[sec])
                    cfg = merged
        except Exception:
            # if file missing or parse error, fall back to defaults + env
            source["defaults"] = True

    # ENV overrides (last)
    envmap = _env_override_map()
    if envmap:
        cfg = _merge(cfg, envmap)
        source["env_overrides"] = True

    return {"policy": cfg, "_source": source}

# --------------------------- Symbol helpers ---------------------------

_PAIR_RE = re.compile(r"^([A-Z0-9]+)[-/]?([A-Z0-9]+)$")

def _split_symbol(symbol: str, venue: str) -> Tuple[str, str]:
    s = (symbol or "").upper().replace(":", "").replace(".", "")
    m = _PAIR_RE.match(s)
    if m:
        return m.group(1), m.group(2)
    if "-" in s:
        a, b = s.split("-", 1)
        return a, b
    # naive fallback
    return s, "USD"

def _join_symbol(base: str, quote: str, venue: str) -> str:
    v = (venue or "").upper()
    if v in ("COINBASE","COINBASEADV","CBADV"):
        return f"{base}-{quote}"
    return f"{base}{quote}"

# --------------------------- Utility ---------------------------

def _float(x: Any, default: Optional[float]=None) -> Optional[float]:
    try: return float(x)
    except Exception: return default

def _get_quote_reserve_usd(context: Optional[dict], venue: str, quote: str) -> Optional[float]:
    if not context: return None
    tel = context.get("telemetry") or {}
    balances = (tel.get("by_venue") or {}).get(venue.upper()) or {}
    if quote in balances:
        return _float(balances.get(quote), 0.0)
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

# --------------------------- Engine ---------------------------

class Engine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or _DEFAULT["policy"]
        self._source = {}

    def evaluate_intent(self, intent: Dict[str, Any], context: Optional[dict]=None) -> Dict[str, Any]:
        cfg = self.cfg
        venue = (intent.get("venue") or "").upper()
        symbol = (intent.get("symbol") or "").upper()
        side   = (intent.get("side") or "").lower()
        amount = _float(intent.get("amount"), 0.0) or 0.0

        # Blocks
        base, quote = _split_symbol(symbol, venue)
        if base in [s.upper() for s in (cfg.get("blocked_symbols") or [])]:
            decision = {"ok": False, "reason": f"blocked symbol {base}",
                        "patched_intent": {}, "flags": ["blocked"]}
            _policy_log(decision=decision, intent=intent, when=None)
            return decision

        # Prefer quote per venue (e.g., COINBASE=USDC, KRAKEN=USDT)
        patched: Dict[str, Any] = {}
        flags: list = []
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

        # Buy-side checks & auto-resize
        if side == "buy":
            # require price to compute amount sizing; optionally deny if unknown
            if price is None and not cfg.get("allow_price_unknown", False):
                decision = {"ok": False, "reason": "price unknown; sizing requires price",
                            "patched_intent": patched, "flags": flags + ["price_unknown"]}
                _policy_log(decision=decision, intent=intent, when=None)
                return decision

            if isinstance(quote_reserve, (int, float)):
                # if total quote below minimum reserve -> deny (or resize-to-zero)
                if min_reserve and float(quote_reserve) < float(min_reserve):
                    patched["amount"] = 0.0
                    decision = {"ok": False, "reason": f"quote below min reserve (${quote_reserve:.2f} < ${min_reserve:.2f})",
                                "patched_intent": patched, "flags": flags + ["below_min_reserve"]}
                    _policy_log(decision=decision, intent=intent, when=None)
                    return decision

                # compute usable quote (respect keepback + canary)
                usable = max(0.0, float(quote_reserve) - (keepback or 0.0))
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
                            decision = {"ok": False,
                                        "reason": f"insufficient quote: have ${quote_reserve:.2f}, usable ${usable:.2f}",
                                        "patched_intent": patched, "flags": flags + ["insufficient_quote"]}
                            _policy_log(decision=decision, intent=intent, when=None)
                            return decision
                else:
                    flags.append("price_unknown")

        # Final decision
        decision = {"ok": True, "reason": "ok", "patched_intent": patched, "flags": flags}
        _policy_log(decision=decision, intent=intent, when=None)
        return decision

# --------------------------- Public API ---------------------------

def load_policy(path: str):
    loaded = _load_yaml(os.getenv("POLICY_PATH") or path or "policy.yaml")
    eng = Engine(loaded.get("policy") or dict(_DEFAULT["policy"]))
    eng._source = loaded.get("_source", {})
    return eng

# Singleton fallbacks the Bus might call directly
_engine_singleton: Optional[Engine] = None

def evaluate_intent(intent: Dict[str, Any], context: Optional[dict]=None) -> Dict[str, Any]:
    global _engine_singleton
    if _engine_singleton is None:
        cfg = (_load_yaml(os.getenv("POLICY_PATH") or "policy.yaml") or {}).get("policy") or _DEFAULT["policy"]
        _engine_singleton = Engine(cfg)
    return _engine_singleton.evaluate_intent(intent, context=context)

def evaluate(intent: Dict[str, Any]) -> Dict[str, Any]:
    return evaluate_intent(intent, context=None)
