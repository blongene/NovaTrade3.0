# enqueue_dummy.py
import os, json
from outbox_db import enqueue
cmd_id = enqueue(
    agent_id = os.getenv("AGENT_ID") or os.getenv("EDGE_AGENT_ID")
    if not agent_id:
        raise SystemExit("AGENT_ID required"),
    kind="order.place",
    payload={"venue":"MEXC","symbol":"MX/USDT","side":"BUY","amount":"5","mode":"market"},
    dedupe_key="smoke-1"
)
print("enqueued", cmd_id)
