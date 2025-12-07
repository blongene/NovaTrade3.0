# policy_engine.py — Phase 8B-ready + Phase 20 Wave 2 (PolicyDecision integration)
from __future__ import annotations
import os, json, hmac, hashlib
from typing import Any, Dict, Optional, Tuple

from policy_decision import PolicyDecision  # Phase 20: canonical decision wrapper

# Prefer Unified_Snapshot (via venue_budget) for quote reserves when available
try:
    from venue_budget import get_quote_equity_usd as _vs_get_quote_equity_usd
except Exception:  # degrade safely if venue_budget is missing
    _vs_get_quote_equity_usd = None
    
# --------------------------- OPTIONAL LOGGERS (degrade safely) ---------------------------
# Existing structured policy logger (if present)
try:
    from policy_logger import log_decision as _policy_log
except Exception:  # pragma: no cover
    def _policy_log(*args, **kwargs):
        return

# Council Ledger (Phase 8B). Best-effort; never breaks flow if missing.
def _ledger(event: str, ok: bool, reason: str = "", token: str = "", action: str = "",
            amt_usd: Any = "", venue: str = "", quote: str = "", patched_json: str = "", ref: str = ""):
    try:
        from council_ledger import log_reckoning
        log_reckoning(event, ok, reason, token, action, amt_usd, venue, quote, patched_json, ref)
    except Exception:
        pass

# --------------------------- Helpers ---------------------------

def _get(d, path, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur: return default
        cur = cur[p]
    return cur

def _venue_min_notional(cfg, venue: str) -> float:
    """Optional per-venue min notional USD gate (e.g., {'BINANCEUS': 10, 'KRAKEN': 5})"""
    try:
        return float((_get(cfg, "venue_min_notional_usd") or {}).get(venue.upper(), 0))
    except Exception:
        return 0.0

def _notional_usd(intent: dict) -> Optional[float]:
    """Prefer explicit notional_usd; else price_usd * amount if both present."""
    n = intent.get("notional_usd")
    if isinstance(n, (int, float)): return float(n)
    p, a = intent.get("price_usd"), intent.get("amount")
    if isinstance(p, (int, float)) and isinstance(a, (int, float)):
        return float(p) * float(a)
    return None

def _abs_path(p: str) -> str:
    if not p: return ""
    return p if os.path.isabs(p) else os.path.abspath(p)

def _merge(dst: dict, src: dict) -> dict:
    """shallow merge; src wins"""
    out = dict(dst or {})
    for k, v in (src or {}).items():
        out[k] = v
    return out

def _env_override_map() -> dict:
    """
    ENV overrides applied LAST (after YAML). All keys optional.
      POLICY_MAX_PER_COIN_USD
      POLICY_MIN_QUOTE_RESERVE_USD
      POLICY_KEEPBACK_USD
      POLICY_CANARY_MAX_USD
      POLICY_ALLOW_PRICE_UNKNOWN   (true/false)
      POLICY_PREFER_QUOTES_JSON    (json: {"BINANCEUS":"USDT","COINBASE":"USDC"})
      POLICY_VENUE_MIN_NOTIONAL_JSON (json: {"BINANCEUS":10,"KRAKEN":5})
    """
    m = {}
    def _f(name, key, cast=float):
        val = os.getenv(name)
        if val is not None:
            try:
                m[key] = cast(val)
            except Exception:
                m[key] = val
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
    vmin = os.getenv("POLICY_VENUE_MIN_NOTIONAL_JSON")
    if vmin:
        try: m["venue_min_notional_usd"] = json.loads(vmin)
        except Exception: pass

    order = os.getenv("POLICY_VENUE_ORDER_JSON")
    if order:
        try: m["venue_order"] = json.loads(order)
        except Exception: pass
    return m

def _get_min_qty_floor(cfg: dict, venue: str, symbol: str) -> Optional[float]:
    """
    Enforce exchange min-qty floors (e.g., {'KRAKEN:BTC-USDT': 0.0001} or {'KRAKEN:BTCUSDT': 0.0001})
    Format key as VENUE:SYMBOL (joined form).
    """
    floors = cfg.get("min_qty_floors") or {}
    key = f"{venue.upper()}:{symbol.upper()}"
    v = floors.get(key)
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

# --------------------------- Defaults ---------------------------

_DEFAULT = {
    "policy": {
        # Pair/venue rules
        "prefer_quotes": {"BINANCEUS":"USDT","COINBASE":"USD","KRAKEN":"USDT"},
        "blocked_symbols": ["BARK","BONK"],

        # Risk & sizing
        "max_per_coin_usd": 25,      # hard cap per intent
        "min_quote_reserve_usd": 25, # deny/resize if quote below this floor
        "keepback_usd": 5,           # never breach this reserve
        "canary_max_usd": 11,        # first sizing cap for safety
        "on_short_quote": "resize",  # resize | deny

        # Price/telemetry expectations
        "allow_price_unknown": False,     # deny if no price and sizing needed

        # Cooldowns (stub; computed elsewhere / Policy_Log assisted)
        "cool_off_minutes_after_trade": 30,

        # Router preference order (if used upstream)
        "venue_order": ["COINBASE","BINANCEUS","KRAKEN"],

        # Optional advanced knobs
        # "venue_min_notional_usd": {"BINANCEUS":10, "KRAKEN":5}
        # "min_qty_floors": {"KRAKEN:BTC-USDT": 0.0001}
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
                    merged = dict(cfg)
                    # safe, shallow merge of known keys
                    for k in (
                        "prefer_quotes", "blocked_symbols",
                        "max_per_coin_usd", "min_quote_reserve_usd",
                        "keepback_usd", "canary_max_usd", "on_short_quote",
                        "allow_price_unknown", "cool_off_minutes_after_trade",
                        "venue_order", "min_liquidity_usd",
                        "venue_min_notional_usd",
                        "min_qty_floors",
                    ):
                        if k in node:
                            merged[k] = node[k]
                    # optional sections pass-through (shallow)
                    for sec in ("risk","execution","caps","cooldowns"):
                        if isinstance(node.get(sec), dict):
                            merged = _merge(merged, node[sec])
                    cfg = merged
        except Exception:
            source["defaults"] = True

    # ENV overrides (last)
    envmap = _env_override_map()
    if envmap:
        cfg = _merge(cfg, envmap)
        source["env_overrides"] = True

    return {"policy": cfg, "_source": source}

# --------------------------- Symbol helpers ---------------------------

def _split_symbol(symbol: str, venue: str) -> Tuple[str, str]:
    s = (symbol or "").upper().replace(":", "").replace(".", "")
    # explicit separators
    if "-" in s:
        a, b = s.split("-", 1)
        return a, b
    if "/" in s:
        a, b = s.split("/", 1)
        return a, b
    # compact suffix detection (common 4-char then 3-char quotes)
    for q in ("USDT", "USDC", "BUSD", "TUSD"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    for q in ("USD", "EUR", "BTC", "ETH"):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)], q
    # default by venue (fallback)
    default_q = {"COINBASE":"USDC","COINBASEADV":"USDC","CBADV":"USDC",
                 "KRAKEN":"USDT","BINANCEUS":"USDT"}.get((venue or "").upper(), "USD")
    return s, default_q

def _join_symbol(base: str, quote: str, venue: str) -> str:
    v = (venue or "").upper()
    if v in ("COINBASE","COINBASEADV","CBADV"):
        return f"{base}-{quote}"
    return f"{base}{quote}"

# --------------------------- Utility ---------------------------

def _float(x: Any, default: Optional[float]=None) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return default

def _get_quote_reserve_usd(context: Optional[dict], venue: str, quote: str) -> Optional[float]:
    """
    Resolve quote reserve for policy decisions.

    Phase 18 behavior:
      1) Prefer Unified_Snapshot via venue_budget.get_quote_equity_usd()
         (this is the same data used by venue_budget / trade_guard).
      2) Fall back to context["telemetry"] (Bus _last_tel shape) if snapshot
         data is missing or venue_budget is unavailable.

    context shape (legacy telemetry path):
        context = {
            "telemetry": {
                "by_venue": {"KRAKEN": {"USDT": 123.4, ...}, ...},
                "flat": {"USDT": 321.0, ...}
            }
        }
    """
    v = (venue or "").upper()
    q = (quote or "").upper()
    if not v or not q:
        return None

    # --- Preferred source: Unified_Snapshot via venue_budget ---
    try:
        if _vs_get_quote_equity_usd:
            eq = _vs_get_quote_equity_usd(v, q)
            if isinstance(eq, (int, float)):
                return float(eq)
    except Exception:
        # Any issues here should silently fall back to telemetry
        pass

    # --- Fallback: legacy telemetry context (wsgi._last_tel) ---
    if not context:
        return None

    tel = context.get("telemetry") or {}

    # Per-venue quote balances
    balances = (tel.get("by_venue") or {}).get(v) or {}
    if q in balances:
        return _float(balances.get(q), 0.0)

    # Flat (global) quote balances, if present
    flat = tel.get("flat") or {}
    if q in flat:
        return _float(flat.get(q), 0.0)

    return None

def _cap_notional(cfg: dict, notional: Optional[float]) -> Tuple[Optional[float], list]:
    flags = []
    cap = _float(cfg.get("max_per_coin_usd"))
    if cap and (notional is not None) and notional > cap:
        flags.append("clamped")
        return cap, flags
    return notional, flags

# --------------------------- PolicyDecision integration ---------------------------

def _attach_policy_decision(
    intent: Dict[str, Any],
    decision: Dict[str, Any],
    *,
    venue: str,
    base: str,
    quote: str,
) -> Dict[str, Any]:
    """
    Phase 20: Wrap a raw decision dict with a PolicyDecision object.

    - Keeps the existing decision shape (ok, reason, patched_intent, flags).
    - Adds:
        * decision_id
        * created_at
        * meta: {venue, symbol, base, quote, requested_amount_usd,
                 approved_amount_usd, limits_applied, council_trace}
        * intent / patched (canonical fields for other subsystems)

    If anything goes wrong, we fall back to the original decision dict.
    """
    try:
        ok = bool(decision.get("ok", True))
        reason = decision.get("reason", "")
        status = "ok" if ok else "blocked"

        patched_intent = decision.get("patched_intent") or {}
        flags = decision.get("flags") or []

        pd = PolicyDecision(
            ok=ok,
            status=status,
            reason=reason,
            intent=dict(intent),
            patched=dict(patched_intent),
            source=str(intent.get("source") or ""),
            venue=venue,
            symbol=str(intent.get("symbol") or ""),
            base=base,
            quote=quote,
            requested_amount_usd=_notional_usd(intent),
            approved_amount_usd=_notional_usd(patched_intent) if patched_intent else None,
            limits_applied=list(flags),
            council_trace={
                "astraeus": {
                    "role": "path",
                    "stage": "post_engine",
                }
            },
        )

        pd_dict = pd.to_dict()
        # Merge raw decision on top so legacy fields stay exactly as they were
        merged = dict(pd_dict)
        merged.update(decision)
        # Ensure patched_intent is always present alongside 'patched'
        merged.setdefault("patched_intent", patched_intent)
        return merged
    except Exception:
        return decision

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

        # Normalize symbol, extract base/quote
        base, quote = _split_symbol(symbol, venue)

        # Blocklist guard
        if base in [s.upper() for s in (cfg.get("blocked_symbols") or [])]:
            decision = {
                "ok": False,
                "reason": f"blocked symbol {base}",
                "patched_intent": {},
                "flags": ["blocked"],
            }
            decision = _attach_policy_decision(intent, decision, venue=venue, base=base, quote=quote)
            _policy_log(decision=decision, intent=intent, when=None)
            _ledger("policy_check", False, f"blocked symbol {base}", base, side,
                    intent.get("notional_usd",""), venue, quote, "", "")
            return decision

        # Normalize final symbol form for venue
        symbol = _join_symbol(base, quote, venue)

        # Prefer quote per venue
        patched: Dict[str, Any] = {}
        flags: list = []
        prefer_quote = (cfg.get("prefer_quotes") or {}).get(venue)
        if prefer_quote and quote != prefer_quote:
            quote = prefer_quote
            patched["symbol"] = _join_symbol(base, quote, venue)
            flags.append("prefer_quote")

        # Price / notional derivation
        price = _float(intent.get("price_usd"))
        notional = _float(intent.get("notional_usd"))
        if notional is None and price is not None:
            notional = price * amount
        elif notional is None and price is None:
            flags.append("notional_unknown")

        # Venue min-notional guard
        min_notional = _venue_min_notional(cfg, venue)
        n = _notional_usd(intent)
        if min_notional and n is not None and n < min_notional:
            decision = {
                "ok": False,
                "reason": f"min notional {min_notional:.2f} USD not met (got {n:.2f})",
                "patched_intent": {},
                "flags": ["below_min_notional"],
            }
            decision = _attach_policy_decision(intent, decision, venue=venue, base=base, quote=quote)
            _policy_log(decision=decision, intent=intent, when=None)
            _ledger("policy_check", False, decision["reason"], base, side, n, venue, quote, "", "")
            return decision

        # Notional cap (max_per_coin_usd)
        notional, clamp_flags = _cap_notional(cfg, notional)
        flags += clamp_flags
        if "clamped" in clamp_flags and price:
            patched["amount"] = round(notional / price, 8)

        # Quote-reserve logic (uses context telemetry if available)
        min_reserve = _float(cfg.get("min_quote_reserve_usd"))
        keepback = _float(cfg.get("keepback_usd"), 0.0) or 0.0
        on_short = (cfg.get("on_short_quote") or "resize").lower()

        quote_reserve = intent.get("quote_reserve_usd")
        if quote_reserve is None:
            quote_reserve = _get_quote_reserve_usd(context, venue, quote)
            if quote_reserve is None:
                flags.append("reserve_unknown")

        if side == "buy":
            if price is None and not cfg.get("allow_price_unknown", False):
                decision = {
                    "ok": False,
                    "reason": "price unknown; sizing requires price",
                    "patched_intent": patched,
                    "flags": flags + ["price_unknown"],
                }
                decision = _attach_policy_decision(intent, decision, venue=venue, base=base, quote=quote)
                _policy_log(decision=decision, intent=intent, when=None)
                _ledger("policy_check", False, decision["reason"], base, side,
                        intent.get("notional_usd",""), venue, quote,
                        json.dumps(patched) if patched else "", "")
                return decision

            if isinstance(quote_reserve, (int, float)):
                if min_reserve and float(quote_reserve) < float(min_reserve):
                    patched["amount"] = 0.0
                    decision = {
                        "ok": False,
                        "reason": f"quote below min reserve (${quote_reserve:.2f} < ${min_reserve:.2f})",
                        "patched_intent": patched,
                        "flags": flags + ["below_min_reserve"],
                    }
                    decision = _attach_policy_decision(intent, decision, venue=venue, base=base, quote=quote)
                    _policy_log(decision=decision, intent=intent, when=None)
                    _ledger("policy_check", False, decision["reason"], base, side,
                            intent.get("notional_usd",""), venue, quote,
                            json.dumps(patched) if patched else "", "")
                    return decision

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
                            decision = {
                                "ok": False,
                                "reason": (
                                    f"insufficient quote: have ${quote_reserve:.2f}, "
                                    f"usable ${usable:.2f}"
                                ),
                                "patched_intent": patched,
                                "flags": flags + ["insufficient_quote"],
                            }
                            decision = _attach_policy_decision(intent, decision, venue=venue, base=base, quote=quote)
                            _policy_log(decision=decision, intent=intent, when=None)
                            _ledger("policy_check", False, decision["reason"], base, side,
                                    intent.get("notional_usd",""), venue, quote,
                                    json.dumps(patched) if patched else "", "")
                            return decision
                else:
                    flags.append("price_unknown")

        # Exchange min-qty floors (enforced on BUY to avoid venue rejects)
        min_floor = _get_min_qty_floor(cfg, venue, _join_symbol(base, quote, venue))
        if min_floor and side == "buy":
            amt = _float(patched.get("amount", amount), amount)
            if amt and amt < min_floor:
                patched["amount"] = float(f"{min_floor:.8f}")
                flags.append("min_qty_floor")

        decision = {
            "ok": True,
            "reason": "ok",
            "patched_intent": patched,
            "flags": flags,
        }
        decision = _attach_policy_decision(intent, decision, venue=venue, base=base, quote=quote)
        _policy_log(decision=decision, intent=intent, when=None)
        _ledger(
            "policy_check",
            True,
            "ok",
            base,
            side,
            decision["patched_intent"].get("notional_usd", intent.get("notional_usd","")),
            venue,
            quote,
            json.dumps(patched) if patched else "",
            "",
        )
        return decision

# --------------------------- Public API ---------------------------

def load_policy(path: str):
    loaded = _load_yaml(os.getenv("POLICY_PATH") or path or "policy.yaml")
    eng = Engine(loaded.get("policy") or dict(_DEFAULT["policy"]))
    eng._source = loaded.get("_source", {})
    return eng

_engine_singleton: Optional[Engine] = None

def _default_context() -> Optional[dict]:
    """
    Build a default policy context using Bus telemetry if available.
    Expected Bus global: wsgi._last_tel = {"by_venue":{...}, "flat":{...}, "ts": ...}
    """
    try:
        import wsgi as bus  # lazy import to avoid hard circulars
        tel = getattr(bus, "_last_tel", None) or {}
        if not isinstance(tel, dict) or (not tel.get("by_venue") and not tel.get("flat")):
            return None
        return {"telemetry": tel}
    except Exception:
        return None

def evaluate_intent(intent: Dict[str, Any], context: Optional[dict]=None) -> Dict[str, Any]:
    global _engine_singleton
    if _engine_singleton is None:
        loaded = _load_yaml(os.getenv("POLICY_PATH"))
        cfg = loaded.get("policy") if loaded else _DEFAULT["policy"]
        _engine_singleton = Engine(cfg)

    # If caller didn’t pass a context, use Bus telemetry by default
    if context is None:
        context = _default_context()

    return _engine_singleton.evaluate_intent(intent, context=context)

def evaluate(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Backwards-compat wrapper: evaluates with default context (Bus telemetry if present).
    """
    return evaluate_intent(intent, context=None)

from typing import Any, Dict, Optional


def evaluate_manual_rebuy(
    *,
    token: str,
    venue: str,
    quote_size: float,
    base_size: float,
    price: float,
    quote: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Backwards-compat helper for manual rebuy flows.

    NovaTrigger / route_manual currently calls this as:

        evaluate_manual_rebuy(
            token=intent.token,
            venue=intent.venue,
            quote_size=size_quote,
            base_size=size_base,
            price=price,
        )

    We translate that into a generic trade intent and pass it to evaluate_intent().
    """
    token_u = (token or "").upper()
    venue_u = (venue or "").upper()
    quote_u = (quote or "USDT").upper()

    # Manual rebuy is always a BUY
    side = "buy"

    # Engine expects symbols like "BTC/USDT"
    symbol = f"{token_u}/{quote_u}"

    # Build an intent for the main policy engine
    intent: Dict[str, Any] = {
        "venue": venue_u,
        "symbol": symbol,
        "side": side,
        "amount": float(base_size or 0.0),
        "notional_usd": float(quote_size or 0.0),
        "price_usd": float(price or 0.0),
        "kind": "manual_rebuy",
    }
    # Carry through any extra fields if present
    intent.update({k: v for k, v in kwargs.items() if k not in intent})

    decision = evaluate_intent(intent, context=None)

    # Ensure patched_intent exists and has the fields manual flows expect
    patched = decision.get("patched_intent") or {}
    decision["patched_intent"] = patched

    patched.setdefault("venue", venue_u)
    patched.setdefault("symbol", symbol)
    patched.setdefault("amount", float(base_size or 0.0))
    patched.setdefault("notional_usd", float(quote_size or 0.0))
    patched.setdefault("token", token_u)
    patched.setdefault("quote", quote_u)

    return decision

# ---------- Backward-compat wrapper for older imports ----------
class PolicyEngine:
    """
    Legacy wrapper so older code can keep: from policy_engine import PolicyEngine
    API kept: pe = PolicyEngine(); ok, reason, patched = pe.validate(intent, asset_state)
    """
    def __init__(self, path: str = None):
        loaded = _load_yaml(os.getenv("POLICY_PATH") or path or "policy.yaml")
        self._eng = Engine(loaded.get("policy") or dict(_DEFAULT["policy"]))
        self.cfg = self._eng.cfg
        # preserve attribute used elsewhere
        self.cooldown_min = int(self.cfg.get("cool_off_minutes_after_trade", 30))

    def validate(self, intent: dict, asset_state: dict | None = None):
        """
        intent (legacy): { token, action, amount_usd?, amount?, venue, quote?, price_usd? }
        asset_state (legacy): may include liquidity_usd, etc. (ignored here)
        Returns: (ok:bool, reason:str, patched_intent:dict)
        """
        token = (intent.get("token") or "").upper()
        venue = (intent.get("venue") or "").upper()
        quote = (intent.get("quote") or "")
        side  = (intent.get("action") or "").lower()

        # Build a symbol for the new engine; prefer explicit 'symbol' if provided
        symbol = intent.get("symbol")
        if not symbol:
            if token and quote:
                symbol = f"{token}/{quote}"
            elif token:
                symbol = token

        # Map legacy fields into the new engine's intent shape
        new_intent = {
            "venue": venue,
            "symbol": symbol,
            "side": side,  # 'buy' or 'sell'
        }
        # propagate metadata for logging (Intent_ID, Source, etc.)
        for k in ("id", "agent_target", "source", "policy_id"):
            if k in intent:
                new_intent[k] = intent.get(k)

        # sizing: prefer explicit notional_usd, else amount & price
        if "notional_usd" in intent:
            new_intent["notional_usd"] = intent.get("notional_usd")
        elif "amount_usd" in intent and "price_usd" in intent and intent.get("price_usd"):
            # convert amount_usd to base 'amount' if price known
            try:
                new_intent["amount"] = float(intent["amount_usd"]) / float(intent["price_usd"])
                new_intent["price_usd"] = float(intent["price_usd"])
            except Exception:
                pass
        else:
            # pass through raw fields if present
            if "amount" in intent: new_intent["amount"] = intent.get("amount")
            if "price_usd" in intent: new_intent["price_usd"] = intent.get("price_usd")

        # 🔁 NEW: use default telemetry context (wallet balances from Bus)
        ctx = _default_context()

        decision = self._eng.evaluate_intent(new_intent, context=ctx)
        ok = bool(decision.get("ok"))
        reason = decision.get("reason", "ok")
        patched = dict(intent)

        # propagate any patched fields back into legacy shape
        p = decision.get("patched_intent") or {}
        if "symbol" in p:
            b, q = _split_symbol(p["symbol"], venue)
            if b: patched["token"] = b
            if q: patched["quote"] = q
        if "amount" in p:
            patched["amount"] = p["amount"]
            # add convenience amount_usd if price known
            try:
                if "price_usd" in intent and intent["price_usd"]:
                    patched["amount_usd"] = float(p["amount"]) * float(intent["price_usd"])
            except Exception:
                pass

        return ok, reason, patched
