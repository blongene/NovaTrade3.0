# emitters.py — strategy → outbox
import os, time, hashlib
from utils import info
from outbox_db import enqueue

AGENT_ID = os.getenv("AGENT_ID", "orion-local")
EMIT_ENABLED = os.getenv("EMIT_ENABLED", "0").lower() in {"1","true","yes"}

def _dedupe(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()[:16]

def emit_order(symbol: str, side: str, quote_amount: float,
               venue: str = "MEXC", mode: str = "market",
               not_before: int | None = None, ttl_s: int = 300):
    if not EMIT_ENABLED:
        info(f"emit skipped (EMIT_ENABLED=0): {symbol} {side} ${quote_amount}")
        return None
    payload = {"venue": venue, "symbol": symbol, "side": side,
               "quote_amount": float(quote_amount), "mode": mode}
    key = _dedupe(f"{venue}|{symbol}|{side}|{int(float(quote_amount))}")
    cid = enqueue(agent_id=AGENT_ID, kind="order.place", payload=payload,
                  dedupe_key=key, not_before=int(time.time()) if not_before is None else not_before, ttl_s=ttl_s)
    info(f"emit → cmd#{cid} {venue} {symbol} {side} ${quote_amount}")
    return cid
