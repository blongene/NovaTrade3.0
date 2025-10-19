# ops_api_sqlite.py — Durable outbox + receipts using SQLite (local disk)
# Endpoints:
#   POST /api/ops/enqueue      (HMAC header)
#   POST /api/commands/pull
#   POST /api/commands/ack     (HMAC header)
#   POST /api/receipts/ack     (HMAC header, optional)
#
# Env (Bus):
#   OUTBOX_DB_PATH=/data/outbox.db
#   OUTBOX_SECRET=<shared secret with Edge>
#
import os, json, hmac, hashlib, sqlite3
from flask import Blueprint, request, jsonify

OPS = Blueprint("ops_api_sqlite_v1", __name__, url_prefix="/api")

DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
SECRET  = (os.getenv("OUTBOX_SECRET") or "").encode("utf-8")

def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    return c

def _init_db():
    c = _conn()
    c.execute("""create table if not exists outbox(
        id integer primary key autoincrement,
        ts text not null default (datetime('now')),
        status text not null default 'NEW',
        payload text not null
    );""")
    c.execute("create index if not exists outbox_status_idx on outbox(status);")
    c.execute("""create table if not exists receipts(
        id integer primary key autoincrement,
        ts text not null default (datetime('now')),
        payload text not null
    );""")
    c.commit(); c.close()
    print(f"[WEB] SQLite outbox ready at {DB_PATH}")

@OPS.record_once
def _bootstrap(state):
    try: _init_db()
    except Exception as e: print(f"[API] outbox db init skipped: {e}")

def _bad(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code

def _verify_hmac(raw: bytes, provided: str) -> bool:
    if not SECRET:
        print("[API] WARNING: OUTBOX_SECRET not set; HMAC disabled.")
        return True
    mac = hmac.new(SECRET, raw, hashlib.sha256).hexdigest()
    if provided.lower().startswith("sha256="):
        provided = provided.split("=",1)[1]
    return hmac.compare_digest(mac, provided.lower())

@OPS.route("/ops/enqueue", methods=["POST"])
def ops_enqueue():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature","")
    if not _verify_hmac(raw, sig): return _bad("invalid signature", 401)
    try: body = json.loads(raw.decode("utf-8"))
    except Exception: return _bad("invalid json body", 400)
    venue  = (body.get("venue") or "").upper()
    symbol = body.get("symbol") or ""
    side   = (body.get("side") or "").upper()
    if not (venue and symbol and side in ("BUY","SELL")):
        return _bad("missing or invalid fields: venue/symbol/side", 422)
    c = _conn(); cur = c.cursor()
    cur.execute("insert into outbox (payload) values (?)", [json.dumps(body)])
    oid = cur.lastrowid
    c.commit(); c.close()
    return jsonify({"ok": True, "enqueued": True, "id": oid}), 200

@OPS.route("/commands/pull", methods=["POST"])
def commands_pull():
    j = request.get_json(silent=True) or {}
    limit = max(1, min(int(j.get("limit") or 10), 50))
    c = _conn(); cur = c.cursor()
    cur.execute("select id, ts, payload from outbox where status='NEW' order by id asc limit ?", [limit])
    rows = [{"id": r[0], "ts": None, "payload": json.loads(r[2])} for r in cur.fetchall()]
    c.close()
    return jsonify({"ok": True, "commands": rows}), 200

@OPS.route("/commands/ack", methods=["POST"])
def commands_ack():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature","")
    if not _verify_hmac(raw, sig): return _bad("invalid signature", 401)
    try: j = json.loads(raw.decode("utf-8"))
    except Exception: return _bad("invalid json body", 400)
    cid = j.get("id"); status = (j.get("status") or "").upper()
    if not (cid and status in ("DONE","ERROR","HELD")):
        return _bad("missing or invalid fields: id/status", 422)
    c = _conn(); cur = c.cursor()
    cur.execute("update outbox set status=? where id=?", [status, cid])
    cur.execute("insert into receipts (payload) values (?)", [json.dumps(j)])
    c.commit(); c.close()
    return jsonify({"ok": True}), 200

@OPS.route("/receipts/ack", methods=["POST"])
def receipts_ack():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature","")
    if not _verify_hmac(raw, sig): return _bad("invalid signature", 401)
    try: j = json.loads(raw.decode("utf-8"))
    except Exception: return _bad("invalid json body", 400)
    c = _conn(); cur = c.cursor()
    cur.execute("insert into receipts (payload) values (?)", [json.dumps(j)])
    c.commit(); c.close()
    return jsonify({"ok": True}), 200
