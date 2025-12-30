# health_summary.py — daily technical health summary
import os
import sqlite3
import time
from utils import send_telegram_message, is_cold_boot

try:
    from db_backbone import outbox_stats as _outbox_stats  # type: ignore
except Exception:
    _outbox_stats = None

DB = os.getenv("BUS_TELEMETRY_DB", "bus_telemetry.db")


def _safe_outbox_stats(*args, **kwargs):
    """
    Safely call db_backbone.outbox_stats() if present.
    Always returns a dict with at least {"available": bool}.
    """
    if _outbox_stats is None:
        return {"available": False}
    try:
        d = _outbox_stats(*args, **kwargs)
        if isinstance(d, dict):
            d.setdefault("available", True)
            return d
        return {"available": True, "value": d}
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
    stale_agents = [agent for agent, ts in heartbeats.items() if ts and (now - int(ts) > 3600)]

    mode = os.getenv("REBUY_MODE", "dryrun")

    stats = _safe_outbox_stats() or {}
    q = stats.get("queued", "?")
    l = stats.get("leased", "?")
    d = stats.get("done", "?")
    available = stats.get("available", False)

    msg = (
        "✅ NovaTrade daily health report:\n"
        f"Mode: `{mode}`\n"
        f"Queue q:{q} l:{l} d:{d} • ({'DB backbone' if available else 'no outbox_stats'})\n"
    )

    if stale_agents:
        msg += f"🚨 Stale agents (>1h): {', '.join(stale_agents)}"
    else:
        msg += "All agents reporting as expected."

    send_telegram_message(msg)
    print("Health summary sent.")


if __name__ == "__main__":
    run_health_summary()
