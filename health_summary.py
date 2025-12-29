# health_summary.py — daily technical health summary
import os
import sqlite3
import time
from datetime import datetime, timedelta
from utils import send_telegram_message, is_cold_boot
try:
    from db_backbone import outbox_stats
except Exception:
    outbox_stats = None

DB = os.getenv("BUS_TELEMETRY_DB", "bus_telemetry.db")

def _safe__safe_outbox_stats(*args, **kwargs):
    if outbox_stats is None:
        return {"available": False}
    try:
        return _safe_outbox_stats(*args, **kwargs)
    except Exception:
        return {"available": False}
def _conn():
    return sqlite3.connect(DB, isolation_level=None, timeout=10)

def _get_heartbeats(con):
    cur = con.execute("SELECT agent, MAX(ts) AS ts FROM telemetry_heartbeat GROUP BY agent")
    return {row[0]: row[1] for row in cur.fetchall()}

def run_health_summary():
    """Sends a daily health summary to Telegram."""
    if is_cold_boot():
        print("Skipping health summary during cold boot.")
        return

    try:
        con = _conn()
        heartbeats = _get_heartbeats(con)
        con.close()
    except Exception as e:
        send_telegram_message(f"🚨 Health summary failed: could not query telemetry DB: {e}")
        return

    now = int(time.time())
    stale_agents = []
    for agent, ts in heartbeats.items():
        if now - ts > 3600:  # 1 hour threshold
            stale_agents.append(agent)

    mode = os.getenv('REBUY_MODE','dryrun')

    msg = (
        f"✅ NovaTrade daily health report:\n"
        f"Mode: `{mode}`\n"
    )

    stats = _safe_outbox_stats() or {}
    q = stats.get("queued", "?")
    l = stats.get("leased", "?")
    d = stats.get("done", "?")
    
    msg = f"Queue q:{q} l:{l} d:{d} • (DB backbone)"
    # include this in your Nova Daily telegram body
    if stale_agents:
        msg += f"🚨 Stale agents: {', '.join(stale_agents)}"
    else:
        msg += "All agents reporting as expected."

    send_telegram_message(msg)
    print("Health summary sent.")

if __name__ == '__main__':
    run_health_summary()
