# ops_api.py
import os, json, hmac, hashlib, sqlite3, time
from flask import Blueprint, request, jsonify
OPS = Blueprint("ops", __name__)
SECRET = os.getenv("OUTBOX_SECRET","")
DB = os.getenv("OUTBOX_DB_PATH","/tmp/outbox.sqlite")

def _sig_ok(secret, payload, sig):
    msg = json.dumps(payload, separators=(",",":"), sort_keys=True).encode()
    want = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(want, sig)

def _db():
    conn = sqlite3.connect(DB); conn.execute(
      "CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY, ts INT, payload TEXT, status TEXT, uniq TEXT UNIQUE)"
    ); return conn

@OPS.route("/ops/enqueue", methods=["POST"])
def enqueue():
    data = request.get_json(force=True)
    payload, sig = data.get("payload",{}), data.get("sig","")
    if not _sig_ok(SECRET, payload, sig):
        return jsonify({"ok": False, "err":"bad hmac"}), 401
    uniq = f"{payload.get('symbol')}|{payload.get('side')}|{payload.get('ts')}"
    conn = _db()
    try:
        conn.execute("INSERT INTO outbox(ts,payload,status,uniq) VALUES(?,?,?,?)",
                     (int(time.time()), json.dumps(payload, separators=(",",":")), "NEW", uniq))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # idempotent
    return jsonify({"ok": True})
