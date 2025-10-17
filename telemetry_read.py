from flask import Blueprint, jsonify
import sqlite3, os, json, time

bp = Blueprint("telemetry_read", __name__, url_prefix="/api/telemetry")
DB = os.getenv("BUS_TELEMETRY_DB", "bus_telemetry.db")

def _q(q, args=()):
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    try:
        return con.execute(q, args).fetchall()
    finally:
        con.close()

@bp.get("/last_seen")
def last_seen():
    hb = _q("""SELECT agent, MAX(ts) AS ts, MAX(latency_ms) AS latency_ms
               FROM telemetry_heartbeat GROUP BY agent""")
    tp = _q("""SELECT agent, aggregates_json, MAX(id) AS id
               FROM telemetry_push GROUP BY agent""")
    pushes = {}
    for r in tp:
        try: pushes[r["agent"]] = json.loads(r["aggregates_json"] or "{}")
        except Exception: pushes[r["agent"]] = {}
    out = {
        "heartbeats": [{"agent": r["agent"], "ts": r["ts"], "latency_ms": r["latency_ms"]} for r in hb],
        "pushes": pushes,
        "ts": int(time.time())
    }
    return jsonify(out)
