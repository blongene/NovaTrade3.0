# ops_api.py — Flask blueprint for HMAC-verified enqueue + optional pull/ack
import os, json, hmac, hashlib, time, sqlite3
from flask import Blueprint, request, jsonify

OPS = Blueprint("ops", __name__)

OUTBOX_SECRET = os.getenv("OUTBOX_SECRET","")
OUTBOX_DB_PATH = os.getenv("OUTBOX_DB_PATH","/tmp/outbox.sqlite")
OUTBOX_DB_URL = os.getenv("OUTBOX_DB_URL","")  # optional Postgres

def _sig_ok(secret, payload, sig) -> bool:
    msg = json.dumps(payload, separators=(",",":"), sort_keys=True).encode("utf-8")
    want = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(want, sig)
    except Exception:
        return want == sig

def _conn():
    if not OUTBOX_DB_URL:
        conn = sqlite3.connect(OUTBOX_DB_PATH, check_same_thread=False)
        conn.execute("CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, payload TEXT, status TEXT, uniq TEXT UNIQUE)")
        conn.execute("CREATE TABLE IF NOT EXISTS receipts (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, payload TEXT)")
        conn.commit()
        return conn
    import psycopg2, psycopg2.extras as ex
    conn = psycopg2.connect(OUTBOX_DB_URL)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS outbox (id SERIAL PRIMARY KEY, ts BIGINT, payload JSONB, status TEXT, uniq TEXT UNIQUE)")
        cur.execute("CREATE TABLE IF NOT EXISTS receipts (id SERIAL PRIMARY KEY, ts BIGINT, payload JSONB)")
    conn.commit()
    return conn

def _insert_outbox(conn, payload: dict):
    uniq = f"{payload.get('symbol')}|{payload.get('side')}|{payload.get('ts')}"
    try:
        if OUTBOX_DB_URL:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO outbox(ts,payload,status,uniq) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (int(time.time()), json.dumps(payload, separators=(',',':')), "NEW", uniq))
            conn.commit()
        else:
            conn.execute("INSERT OR IGNORE INTO outbox(ts,payload,status,uniq) VALUES(?,?,?,?)",
                         (int(time.time()), json.dumps(payload, separators=(',',':')), "NEW", uniq))
            conn.commit()
        return True
    except Exception as e:
        return False

@OPS.route("/ops/enqueue", methods=["POST"])
def enqueue():
    data = request.get_json(force=True) or {}
    payload = data.get("payload", {})
    sig     = data.get("sig","")
    if not OUTBOX_SECRET:
        return jsonify({"ok": False, "err": "server missing OUTBOX_SECRET"}), 500
    if not _sig_ok(OUTBOX_SECRET, payload, sig):
        return jsonify({"ok": False, "err": "bad hmac"}), 401
    conn = _conn()
    if _insert_outbox(conn, payload):
        return jsonify({"ok": True})
    return jsonify({"ok": False, "err": "insert failed"}), 500

@OPS.route("/api/commands/pull", methods=["POST"])
def pull():
    limit = int((request.get_json(force=True) or {}).get("limit", 10))
    conn = _conn()
    if OUTBOX_DB_URL:
        import psycopg2.extras as ex
        with conn.cursor(cursor_factory=ex.DictCursor) as cur:
            cur.execute("SELECT id, ts, payload FROM outbox WHERE status='NEW' ORDER BY id ASC LIMIT %s", (limit,))
            rows = cur.fetchall()
        cmds = [{"id": r["id"], "ts": r["ts"], "payload": r["payload"]} for r in rows]
    else:
        cur = conn.execute("SELECT id, ts, payload FROM outbox WHERE status='NEW' ORDER BY id ASC LIMIT ?", (limit,))
        rows = cur.fetchall()
        cmds = [{"id": r[0], "ts": r[1], "payload": json.loads(r[2])} for r in rows]
    return jsonify({"ok": True, "commands": cmds})

@OPS.route("/api/commands/ack", methods=["POST"])
def ack():
    body = request.get_json(force=True) or {}
    cid = body.get("id")
    status = body.get("status","DONE")
    receipt = body.get("receipt", {})
    conn = _conn()
    try:
        if OUTBOX_DB_URL:
            with conn.cursor() as cur:
                cur.execute("UPDATE outbox SET status=%s WHERE id=%s", (status, cid))
                cur.execute("INSERT INTO receipts(ts,payload) VALUES(%s,%s)", (int(time.time()), json.dumps(receipt, separators=(',',':'))))
            conn.commit()
        else:
            conn.execute("UPDATE outbox SET status=? WHERE id=?", (status, cid))
            conn.execute("INSERT INTO receipts(ts,payload) VALUES(?,?)", (int(time.time()), json.dumps(receipt, separators=(',',':'))))
            conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)}), 500
