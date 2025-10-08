# cloud enqueue example
from outbox_db import enqueue

def queue_sell(symbol: str, amount: float, agent_id: str | None = None):
    agent = (agent_id or os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID"))
    if not agent:
        raise RuntimeError("AGENT_ID required")
    cmd_id = enqueue(agent, "rebalance.sell", {"symbol": symbol, "amount": amount},
                     not_before=0, dedupe_key=f"sell:{symbol}:{round(amount,6)}")
    print(f"queued sell #{cmd_id} -> {symbol} amount={amount}")

_enqueue_agent = os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID")
if not _enqueue_agent:
    raise RuntimeError("AGENT_ID required")
enqueue(_enqueue_agent, "ping", {"msg": "hello"})
