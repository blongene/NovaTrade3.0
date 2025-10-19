# ops_api_sqlite.py — Durable outbox + receipts using SQLite (with schema migration & lock retries)
import os, json, hmac, hashlib, sqlite3, time
from flask import Blueprint, request, jsonify

OPS = Blueprint("ops_api_sqlite_v1", __name__, url_prefix="/api")

DB_PATH = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
SECRET  = (os.getenv("OUTBOX_SECRET") or "").encode("utf-8")

def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # timeout -> wait for busy DB; check_same_thread False -> allow multi-threads in Gunicorn worker
    c = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    c.execute("PRAGMA busy_timeout=30000;")  # 30s
    return c

def _exec_retry(fn, retries=5, sleep=0.05):
    """Run a DB fn(c) with retry on SQLITE_BUSY / locked."""
    for i in range(retries):
        c = _conn()
        try:
            rv = fn(c)
            c.commit()
            return rv
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            c.rollback()
            if "database is locked" in msg or "database table is locked" in msg or "busy" in msg:
                time.sleep(sleep * (i + 1))
                continue
            raise
        finally:
            c.close()
    # final attempt (let exception surface if any)
    c = _conn()
    try:
        rv = fn(c)
        c.commit()
        return rv
    finally:
        c.close()

def _init_db():
    def _create(c):
        c.execute("""
        create table if not exists outbox(
          id integer primary key autoincrement,
          ts text not null default (datetime('now')),
          status text not null default 'NEW',
          payload text not null
        );
        """)
        c.execute("create index if not exists outbox_status_idx on outbox(status);")
        c.execute("""
        create table if not exists receipts(
          id integer primary key autoincrement,
          ts text not null default (datetime('now')),
          payload text not null
        );
        """)
    _exec_retry(_create)

def _migrate_receipts_if_needed():
    def _mig(c):
        # check columns on receipts
        cols = [r[1] for r in c.execute("PRAGMA table_info(receipts);").fetchall()]
        if not cols:
            # no table (fresh) -> create in init; nothing to do
            return
        if "payload" in cols:
            return
        # migrate: create receipts_v2 with payload JSON string, copy any minimal info if possible
        c.execute("""
        create table if not exists receipts_v2(
          id integer primary key autoincrement,
          ts text not null default (datetime('now')),
          payload text not null
        );
        """)
        # try to copy old rows as best-effort JSON
        # Assume old table may have columns like id, ts, status, txid, symbol, venue...
        rows = c.execute("select * from receipts").fetchall()
        colinfo = c.execute("PRAGMA table_info(receipts);").fetchall()
        names = [ci[1] for ci in colinfo]
        for row in rows:
            obj = {names[i]: row[i] for i in range(len(names))}
            c.execute("insert into receipts_v2(payload) values (?)", [json.dumps(obj)])
        c.execute("drop table receipts;")
        c.execute("alter table receipts_v2 rename to receipts;")
    _exec_retry(_mig)

@OPS.record_once
def _bootstrap(state):
    try:
        _init_db()
        _migrate_receipts_if_needed()
        print(f"[WEB] SQLite outbox ready at {DB_PATH}")
    except Exception as e:
        print(f"[API] outbox db init skipped: {e}")

def _bad(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code

def _verify_hmac(raw: bytes, provided: str) -> bool:
    if not SECRET:
        print("[API] WARNING: OUTBOX_SECRET not set; HMAC disabled.")
        return True
    mac = hmac.new(SECRET, raw, hashlib.sha256).hexdigest()
    if provided.lower().startswith("sha256="):
        provided = provided.split("=", 1)[1]
    return hmac.compare_digest(mac, provided.lower())

@OPS.route("/ops/enqueue", methods=["POST"])
def ops_enqueue():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature", "")
    if not _verify_hmac(raw, sig): return _bad("invalid signature", 401)
    try: body = json.loads(raw.decode("utf-8"))
    except Exception: return _bad("invalid json body", 400)

    venue  = (body.get("venue") or "").upper()
    symbol = body.get("symbol") or ""
    side   = (body.get("side") or "").upper()
    if not (venue and symbol and side in ("BUY", "SELL")):
        return _bad("missing or invalid fields: venue/symbol/side", 422)

    def _ins(c):
        c.execute("insert into outbox (payload) values (?)", [json.dumps(body)])
        return c.execute("select last_insert_rowid()").fetchone()[0]

    oid = _exec_retry(_ins)
    return jsonify({"ok": True, "enqueued": True, "id": oid}), 200

@OPS.route("/commands/pull", methods=["POST"])
def commands_pull():
    j = request.get_json(silent=True) or {}
    limit = max(1, min(int(j.get("limit") or 10), 50))
    def _sel(c):
        cur = c.execute("select id, ts, payload from outbox where status='NEW' order by id asc limit ?", [limit])
        return [{"id": r[0], "ts": r[1], "payload": json.loads(r[2])} for r in cur.fetchall()]
    rows = _exec_retry(_sel)
    return jsonify({"ok": True, "commands": rows}), 200

@OPS.route("/commands/ack", methods=["POST"])
def commands_ack():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature", "")
    if not _verify_hmac(raw, sig): return _bad("invalid signature", 401)
    try: j = json.loads(raw.decode("utf-8"))
    except Exception: return _bad("invalid json body", 400)

    cid = j.get("id"); status = (j.get("status") or "").upper()
    if not (cid and status in ("DONE", "ERROR", "HELD")):
        return _bad("missing or invalid fields: id/status", 422)

    def _ack(c):
        c.execute("update outbox set status=? where id=?", [status, cid])
        c.execute("insert into receipts (payload) values (?)", [json.dumps(j)])
    _exec_retry(_ack)
    return jsonify({"ok": True}), 200

@OPS.route("/receipts/ack", methods=["POST"])
def receipts_ack():
    raw = request.get_data() or b""
    sig = request.headers.get("X-Outbox-Signature", "")
    if not _verify_hmac(raw, sig): return _bad("invalid signature", 401)
    try: j = json.loads(raw.decode("utf-8"))
    except Exception: return _bad("invalid json body", 400)

    def _ins(c):
        c.execute("insert into receipts (payload) values (?)", [json.dumps(j)])
    _exec_retry(_ins)
    return jsonify({"ok": True}), 200
