# cloud enqueue example
from outbox_db import enqueue

def queue_sell(symbol: str, amount: float, agent_id: str | None = None):
    agent = agent_id or os.getenv("AGENT_ID","orion-local")
    cmd_id = enqueue(agent, "rebalance.sell", {"symbol": symbol, "amount": amount},
                     not_before=0, dedupe_key=f"sell:{symbol}:{round(amount,6)}")
    print(f"queued sell #{cmd_id} -> {symbol} amount={amount}")

enqueue(os.getenv("AGENT_ID","orion-local"), "ping", {"msg": "hello"})
