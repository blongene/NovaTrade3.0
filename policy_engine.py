
# policy_engine.py — Bus contract glue (Phase 6B)
from __future__ import annotations
import os, json, time, re
from typing import Any, Dict, Tuple

try:
    from policy_logger import log_decision as _policy_log
except Exception:
    def _policy_log(*args, **kwargs):
        return

_DEFAULT = {
    "policy": {
        "max_per_coin_usd": 25,
        "min_quote_reserve_usd": 10,
        "min_liquidity_usd": 50_000,
        "cool_off_minutes_after_trade": 30,
        "prefer_quotes": {"BINANCEUS":"USDT","COINBASE":"USDC","KRAKEN":"USDT"},
        "venue_order": ["BINANCEUS","COINBASE","KRAKEN"],
        "blocked_symbols": ["BARK","BONK"]
    }
}

def _load_yaml(path: str) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) and "policy" in data else _DEFAULT
    except Exception:
        pass
    try:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read()
    except Exception:
        return _DEFAULT
    cur = dict(_DEFAULT["policy"])
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    in_policy = False
    for ln in lines:
        if ln.startswith("policy:"):
            in_policy = True
            continue
        if not in_policy or ":" not in ln:
            continue
        k, v = ln.split(":",1); k=k.strip(); v=v.strip()
        if k == "prefer_quotes": cur[k] = {}
        elif k in ("BINANCEUS","COINBASE","KRAKEN"):
            cur.setdefault("prefer_quotes", {})[k] = v
        elif v.lower() in ("true","false"): cur[k] = (v.lower()=="true")
        elif v.startswith("[") and v.endswith("]"):
            inner = v[1:-1].strip()
            cur[k] = [x.strip() for x in inner.split(",")] if inner else []
        else:
            try:
                cur[k] = float(v) if "." in v else int(v)
            except Exception:
                cur[k] = v
    return {"policy": cur}

_PAIR_RE = re.compile(r"^([A-Z0-9]+)[-/]?([A-Z0-9]+)$")

def _split_symbol(symbol: str, venue: str):
    s = (symbol or "").upper().replace(":", "").replace(".", "")
    v = (venue or "").upper()
    m = _PAIR_RE.match(s)
    if m:
        base, quote = m.group(1), m.group(2)
        return base, quote
    if "-" in s:
        base, quote = s.split("-", 1)
        return base, quote
    return s, "USD"

def _join_symbol(base: str, quote: str, venue: str) -> str:
    v = (venue or "").upper()
    if v in ("COINBASE","COINBASEADV","CBADV"):
        return f"{base}-{quote}"
    return f"{base}{quote}"

class _Engine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or _DEFAULT["policy"]

    def evaluate(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        cfg = self.cfg
        venue = (intent.get("venue") or "").upper()
        symbol = (intent.get("symbol") or "").upper()
        side   = (intent.get("side") or "").lower()
        amount = float(intent.get("amount") or 0.0)
        source = (intent.get("source") or "").lower()

        base, quote = _split_symbol(symbol, venue)
        prefer_quote = (cfg.get("prefer_quotes") or {}).get(venue)
        patched: Dict[str, Any] = {}
        flags = []

        blocked = [s.upper() for s in (cfg.get("blocked_symbols") or [])]
        if base in blocked:
            decision = {"allowed": False, "reason": f"blocked symbol {base}", "patched": {}, "flags": ["blocked"]}
            _policy_log(decision=decision, intent=intent, when=None)
            return decision

        if prefer_quote and quote != prefer_quote:
            quote = prefer_quote
            patched["symbol"] = _join_symbol(base, quote, venue)
            flags.append("prefer_quote")

        price = intent.get("price_usd")
        notional = intent.get("notional_usd")
        if notional is None and price is not None:
            try:
                notional = float(price) * float(amount)
            except Exception:
                notional = None

        max_per = cfg.get("max_per_coin_usd")
        if max_per and isinstance(max_per, (int, float)):
            if notional is None:
                flags.append("notional_unknown")
            else:
                if notional > float(max_per) and price:
                    max_amount = float(max_per) / float(price)
                    if max_amount <= 0:
                        decision = {"allowed": False, "reason": "max_per_coin_usd=0", "patched": {}, "flags": ["policy_cap"]}
                        _policy_log(decision=decision, intent=intent, when=None)
                        return decision
                    patched["amount"] = max_amount
                    flags.append("clamped")

        min_reserve = cfg.get("min_quote_reserve_usd")
        if min_reserve and isinstance(min_reserve, (int,float)) and intent.get("quote_reserve_usd") is None:
            flags.append("reserve_unknown")

        decision = {"allowed": True, "reason": "ok", "patched": patched, "flags": flags}
        _policy_log(decision=decision, intent=intent, when=None)
        return decision

def load_policy(path: str):
    data = _load_yaml(path)
    cfg = data.get("policy") or _DEFAULT["policy"]
    return _Engine(cfg)

_engine_singleton = None
def evaluate(intent: Dict[str, Any]) -> Dict[str, Any]:
    global _engine_singleton
    if _engine_singleton is None:
        cfg = (_load_yaml(os.getenv("POLICY_PATH") or "policy.yaml")).get("policy") or _DEFAULT["policy"]
        _engine_singleton = _Engine(cfg)
    return _engine_singleton.evaluate(intent)
