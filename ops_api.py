# ops_api.py — Durable outbox + commands API (Postgres, HMAC)
import os, json, hmac, hashlib, time
from datetime import datetime
import psycopg2, psycopg2.extras
from flask import Blueprint, request, jsonify

OPS = Blueprint("ops_api_v1", __name__, url_prefix="/api")

DB_URL = os.getenv("OUTBOX_DB_URL")
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
    mac = hmac.new(SECRET, raw, hashlib.sha256).hexdigest()
    if provided.lower().startswith("sha256="):
        provided = provided.split("=",1)[1]
    return hmac.compare_digest(mac, provided.lower())

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
    venue = (body.get("venue") or "").upper()
    symbol = body.get("symbol") or ""
    side = (body.get("side") or "").upper()
    amt_q = body.get("amount_quote")
    if not (venue and symbol and side in ("BUY","SELL")):
        return _bad("missing or invalid fields: venue/symbol/side", 422)
    with _conn() as c, c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("insert into outbox (payload) values (%s) returning id, ts",
                    [json.dumps(body)])
        row = cur.fetchone()
    return jsonify({"ok": True, "enqueued": True, "id": row["id"], "ts": row["ts"].isoformat()}), 200
