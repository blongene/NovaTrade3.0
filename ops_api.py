# ops_api.py — Durable outbox + commands API (Postgres, HMAC)
import os, json, hmac, hashlib, time
from datetime import datetime
import psycopg2, psycopg2.extras
from flask import Blueprint, request, jsonify

OPS = Blueprint("ops_api_v1", __name__, url_prefix="/api")

DB_URL = os.getenv("OUTBOX_DB_URL")  # postgres://...
SECRET = (os.getenv("OUTBOX_SECRET") or "").encode("utf-8")

def _conn():
    if not DB_URL:
        raise RuntimeError("OUTBOX_DB_URL not set")
    return psycopg2.connect(DB_URL, sslmode=os.getenv("PGSSLMODE","require"))

def _init_db():
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
        create table if not exists outbox(
          id bigserial primary key,
          ts timestamptz not null default now(),
          status text not null default 'NEW',
          payload jsonb not null
        );
        create index if not exists outbox_status_idx on outbox(status);
        create table if not exists receipts(
          id bigserial primary key,
          ts timestamptz not null default now(),
          payload jsonb not null
        );
        """)
    print("[WEB] Outbox/receipts tables ready.")

@OPS.record_once
def _bootstrap(state):
    try:
        _init_db()
    except Exception as e:
        print(f"[API] outbox db init skipped: {e}")

def _bad(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code

def _canon(d: dict) -> bytes:
    return json.dumps(d, separators=(",", ":"), sort_keys=False).encode("utf-8")

def _verify_hmac(raw: bytes, provided: str) -> bool:
    if not SECRET:
        print("[API] WARNING: OUTBOX_SECRET not set; HMAC disabled.")
        return True
    try:
        mac = hmac.new(SECRET, raw, hashlib.sha256).hexdigest()
        if provided.lower().startswith("sha256="):
            provided = provided.split("=",1)[1]
        return hmac.compare_digest(mac, provided.lower())
    except Exception:
        return False

# ---------- POST /api/ops/enqueue -------------------------------------------
@OPS.route("/ops/enqueue", methods=["POST"])
def ops_enqueue():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature", "")
    if not _verify_hmac(raw, sig):
        return _bad("invalid signature", 401)

    try:
        body = json.loads(raw.decode("utf-8"))
    except Exception:
        return _bad("invalid json body", 400)

    # expected minimal payload
    venue  = (body.get("venue") or "").upper()
    symbol = body.get("symbol") or ""
    side   = (body.get("side") or "").upper()   # BUY or SELL
    amt_q  = body.get("amount_quote")
    if not (venue and symbol and side in ("BUY","SELL")):
        return _bad("missing or invalid fields: venue/symbol/side", 422)

    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("insert into outbox (payload) values (%s) returning id, ts",
                    [json.dumps(body)])
        row = cur.fetchone()

    return jsonify({"ok": True, "enqueued": True, "id": row["id"], "ts": row["ts"].isoformat()}), 200

# ---------- POST /api/commands/pull -----------------------------------------
@OPS.route("/commands/pull", methods=["POST"])
def commands_pull():
    # optional: limit in body
    try:
        j = request.get_json(silent=True) or {}
    except Exception:
        j = {}
    limit = int(j.get("limit") or 10)
    limit = max(1, min(limit, 50))

    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
          select id, extract(epoch from ts)::bigint as ts, payload
          from outbox
          where status='NEW'
          order by id asc
          limit %s
        """, [limit])
        rows = cur.fetchall()

    cmds = [{"id": r["id"], "ts": int(r["ts"]), "payload": r["payload"]} for r in rows]
    return jsonify({"ok": True, "commands": cmds}), 200

# ---------- POST /api/commands/ack ------------------------------------------
@OPS.route("/commands/ack", methods=["POST"])
def commands_ack():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature", "")
    if not _verify_hmac(raw, sig):
        return _bad("invalid signature", 401)

    try:
        j = json.loads(raw.decode("utf-8"))
    except Exception:
        return _bad("invalid json body", 400)

    cid    = j.get("id")
    status = (j.get("status") or "").upper()  # DONE / ERROR / HELD
    receipt = j.get("receipt") or {}
    if not (cid and status in ("DONE","ERROR","HELD")):
        return _bad("missing or invalid fields: id/status", 422)

    with _conn() as c, c.cursor() as cur:
        # update outbox status
        cur.execute("update outbox set status=%s where id=%s", [status, cid])
        # store receipt
        cur.execute("insert into receipts (payload) values (%s)", [json.dumps({"id": cid, "status": status, "receipt": receipt})])

    return jsonify({"ok": True}), 200

# ---------- POST /api/receipts/ack (optional, same as above but separate) ---
@OPS.route("/receipts/ack", methods=["POST"])
def receipts_ack():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature", "")
    if not _verify_hmac(raw, sig):
        return _bad("invalid signature", 401)

    try:
        j = json.loads(raw.decode("utf-8"))
    except Exception:
        return _bad("invalid json body", 400)

    with _conn() as c, c.cursor() as cur:
        cur.execute("insert into receipts (payload) values (%s)", [json.dumps(j)])
    return jsonify({"ok": True}), 200
