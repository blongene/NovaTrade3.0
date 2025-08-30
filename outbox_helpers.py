# outbox_helpers.py
import os
from outbox_db import enqueue

def queue_ping(msg: str = "hello"):
    """Enqueue a ping command for the Edge Agent to test pull/ack."""
    agent = os.getenv("AGENT_ID", "orion-local")
    cmd_id = enqueue(agent, "ping", {"msg": msg})
    print(f"📡 Ping queued → cmd_id={cmd_id} ({msg})")
    return cmd_id
