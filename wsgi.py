# wsgi.py — NovaTrade Bus (final drop-in)
from __future__ import annotations
import os, logging, threading, time, json, uuid, hmac, hashlib, sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
from flask import Flask, jsonify, request, Blueprint
import sheets_mirror_plus as _pd
import receipts_compactor as _rc

# ============================================================================
# Logging
# ============================================================================
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bus")
logging.getLogger("werkzeug").setLevel(logging.WARNING if LOG_LEVEL != "DEBUG" else logging.DEBUG)

# ============================================================================
# Flask app
# ============================================================================
flask_app = Flask(__name__)

# ============================================================================
# Telegram (quiet, optional; never crashes the Bus)
# ============================================================================
try:
    # Prefer the helper inside your telegram_webhook module
    from telegram_webhook import _send_telegram as send_telegram
    log.info("Telegram send_telegram imported from telegram_webhook.py")
except Exception as e:
    log.warning("telegram_webhook import degraded, using fallback: %s", e)
    import requests
    def send_telegram(text: str):
        if os.getenv("ENABLE_TELEGRAM","").lower() not in ("1","true","yes"):
            return
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            log.debug("Telegram not configured; skipping message.")
            return
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text[:4000], "parse_mode": "HTML"},
                timeout=8
            )
            if not r.ok:
                log.warning("Telegram send failed: %s", r.text)
        except Exception as e2:
            log.warning("Telegram send degraded: %s", e2)

def _maybe_init_telegram(app: Flask) -> Optional[str]:
    if os.environ.get("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return None
    try:
        import telegram_webhook as tgmod  # optional user module
        tg_bp = None
        if hasattr(tgmod, "tg_blueprint"):
            tg_bp = getattr(tgmod, "tg_blueprint")
        elif hasattr(tgmod, "telegram_bp"):
            tg_bp = getattr(tgmod, "telegram_bp")
        elif hasattr(tgmod, "create_blueprint") and callable(tgmod.create_blueprint):
            tg_bp = tgmod.create_blueprint()
        if tg_bp is not None:
            from flask import Blueprint as _BP
            if isinstance(tg_bp, _BP):
                app.register_blueprint(tg_bp, url_prefix="/tg")
                log.info("Telegram blueprint mounted at /tg")
            else:
                try:
                    from werkzeug.middleware.dispatcher import DispatcherMiddleware
                    subapp = getattr(tg_bp, "wsgi_app", None)
                    if subapp is None:
                        raise TypeError("telegram object is neither Blueprint nor Flask.wsgi_app")
                    app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/tg": subapp})
                    log.info("Telegram Flask app mounted at /tg")
                except Exception as e:
                    log.warning("Telegram mount degraded: %s", e)
                    return str(e)
        setter = getattr(tgmod, "set_telegram_webhook", None)
        if callable(setter):
            try:
                setter()
            except Exception as e:
                log.info("Telegram webhook setter degraded: %s", e)
        return None
    except Exception as e:
        log.warning("Telegram init failed: %s", e)
        return str(e)

telegram_status = _maybe_init_telegram(flask_app)
# Optional Sheets Gateway mount (uses existing GOOGLE_* / SVC_JSON / SHEET_URL)
try:
    from sheets_bp import SHEETS_ROUTES, start_background_flusher
    flask_app.register_blueprint(SHEETS_ROUTES, url_prefix='/sheets')
    try:
        start_background_flusher()
    except Exception as e:
        log.info('Sheets flusher not started: %s', e)
    log.info('SheetsGateway mounted at /sheets')
except Exception as e:
    log.info('SheetsGateway not mounted: %s', e)


# ============================================================================
# HMAC helpers & policy flags
def _read_secret() -> str:
    import os
    pf = os.getenv('OUTBOX_SECRET_FILE','').strip()
    if pf:
        try:
            return Path(pf).read_text(encoding='utf-8').strip()
        except Exception:
            pass
    return os.getenv('OUTBOX_SECRET','').strip()

# ============================================================================
OUTBOX_SECRET = _read_secret()
REQUIRE_HMAC_OPS      = os.getenv("REQUIRE_HMAC_OPS","0").lower() in ("1","true","yes")
REQUIRE_HMAC_PULL     = os.getenv("REQUIRE_HMAC_PULL","0").lower() in ("1","true","yes")
REQUIRE_HMAC_TELEMETRY= os.getenv("REQUIRE_HMAC_TELEMETRY","0").lower() in ("1","true","yes")
TELEMETRY_SECRET      = os.getenv("TELEMETRY_SECRET", OUTBOX_SECRET)

def _canonical(d: dict) -> bytes:
    return json.dumps(d, separators=(",",":"), sort_keys=True).encode("utf-8")

def _verify_with(secret: str, body: dict, sig: str) -> bool:
    if not secret:
        return True
    try:
        exp = hmac.new(secret.encode("utf-8"), _canonical(body), hashlib.sha256).hexdigest()
        return hmac.compare_digest(exp, sig or "")
    except Exception:
        return False

def _require_json():
    if not request.is_json:
        return None, ("invalid or missing JSON body", 400)
    try:
        return request.get_json(force=True, silent=False), None
    except Exception:
        return None, ("malformed JSON", 400)

def _maybe_require_hmac_ops(body):
    if REQUIRE_HMAC_OPS and not _verify_with(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return ("invalid signature", 401)
    return None

def _maybe_require_hmac_pull(body):
    if REQUIRE_HMAC_PULL and not _verify_with(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return ("invalid signature", 401)
    return None

def _maybe_require_hmac_tel(body):
    if REQUIRE_HMAC_TELEMETRY and not _verify_with(TELEMETRY_SECRET, body, request.headers.get("X-NT-Sig","")):
        return ("invalid signature", 401)
    return None

# ============================================================================
# SQLite durable store (WAL)
# ============================================================================
DB_PATH = os.getenv("OUTBOX_DB_PATH", "./outbox.sqlite")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',   -- queued|leased|acked|failed
  created_at TEXT NOT NULL,
  leased_at TEXT,
  lease_expires_at TEXT,
  agent_id TEXT
);
CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id TEXT NOT NULL,
  agent_id TEXT,
  status TEXT NOT NULL,                    -- ok|error|skipped
  detail TEXT,                             -- JSON
  created_at TEXT NOT NULL,
  FOREIGN KEY(command_id) REFERENCES commands(id)
);
CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status);
CREATE INDEX IF NOT EXISTS idx_commands_lease_exp ON commands(lease_expires_at);
"""

def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def _ensure_receipts_compat(conn: sqlite3.Connection) -> None:
    # Add legacy columns some readers expect (ts, payload)
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(receipts)").fetchall()}
        changed = False
        if "ts" not in cols:
            conn.execute("ALTER TABLE receipts ADD COLUMN ts TEXT")
            changed = True
        if "payload" not in cols:
            conn.execute("ALTER TABLE receipts ADD COLUMN payload TEXT")
            changed = True
        if changed:
            conn.execute("UPDATE receipts SET ts = COALESCE(ts, created_at)")
            conn.execute("UPDATE receipts SET payload = COALESCE(payload, detail)")
            conn.commit()
    except Exception as e:
        log.info("receipts compat check degraded: %s", e)

def _init_db():
    conn = _connect()
    try:
        for stmt in [s.strip() for s in SCHEMA.strip().split(";") if s.strip()]:
            conn.execute(stmt)
        _ensure_receipts_compat(conn)
    finally:
        conn.close()

_init_db()

def _enqueue_command(cmd_id: str, payload: Dict[str, Any]) -> None:
    now = datetime.utcnow().isoformat()
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO commands(id, payload, status, created_at) VALUES (?,?, 'queued', ?)",
            (cmd_id, json.dumps(payload, separators=(",",":")), now)
        )
    finally:
        conn.close()

def _pull_commands(agent_id: str, max_items: int = 10, lease_seconds: int = 90) -> List[Dict]:
    now = datetime.utcnow()
    now_iso = now.isoformat()
    lease_expiry_iso = (now + timedelta(seconds=lease_seconds)).isoformat()
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT id, payload FROM commands
            WHERE status IN ('queued','leased')
              AND (lease_expires_at IS NULL OR lease_expires_at < ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now_iso, max_items)
        ).fetchall()
        if not rows:
            return []
        ids = [r[0] for r in rows]
        qmarks = ",".join("?" for _ in ids)
        conn.execute(
            f"""
            UPDATE commands
               SET status='leased',
                   leased_at=?,
                   lease_expires_at=?,
                   agent_id=?
             WHERE id IN ({qmarks})
            """,
            (now_iso, lease_expiry_iso, agent_id, *ids)
        )
        return [{"id": r[0], "payload": json.loads(r[1])} for r in rows]
    finally:
        conn.close()

def _ack_command(cmd_id: str, agent_id: str, status: str, detail: Optional[Dict] = None) -> None:
    now = datetime.utcnow().isoformat()
    jdetail = json.dumps(detail or {}, separators=(",",":"))
    conn = _connect()
    try:
        _ensure_receipts_compat(conn)
        conn.execute(
            """
            INSERT INTO receipts(command_id, agent_id, status, detail, created_at, ts, payload)
            VALUES (?,?,?,?,?,?,?)
            """,
            (cmd_id, agent_id, status.lower(), jdetail, now, now, jdetail),
        )
        conn.execute("UPDATE commands SET status='acked' WHERE id=?", (cmd_id,))
    finally:
        conn.close()


def _last_receipts(n: int = 10):
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT command_id, agent_id, status, detail, created_at FROM receipts ORDER BY created_at DESC LIMIT ?",
            (int(n),)
        ).fetchall()
        out = []
        for cid, aid, st, detail, ts in rows:
            try:
                import json as _json
                payload = _json.loads(detail) if detail else None
            except Exception:
                payload = {"raw": detail}
            out.append({
                "command_id": cid,
                "agent_id": aid,
                "status": st,
                "payload": payload,
                "ts": ts
            })
        return out
    finally:
        conn.close()


def _queue_depth() -> Dict[str,int]:
    conn = _connect()
    try:
        out = {}
        for st in ("queued","leased","acked","failed"):
            n = conn.execute("SELECT COUNT(1) FROM commands WHERE status=?", (st,)).fetchone()[0]
            out[st] = int(n)
        return out
    finally:
        conn.close()

# ============================================================================
# Health
# ============================================================================
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info: Dict[str, Any] = {"ok": True, "web": "up"}
    info["telegram"] = {"status": "ok" if not telegram_status else "degraded", "reason": telegram_status}
    info["db"] = DB_PATH
    try:
        info["queue"] = _queue_depth()
    except Exception as e:
        info["queue_error"] = str(e)
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True, service="Bus", ready=True), 200

# ============================================================================
# Telemetry (Edge) — with legacy aliases
# ============================================================================
_last_telemetry: Dict[str, Any] = {"agent_id": None, "flat": {}, "by_venue": {}, "ts": 0}

def _safe_float(x) -> float:
    try: return float(x)
    except Exception: return 0.0

def _normalize_balances(raw) -> Tuple[dict, dict]:
    if not isinstance(raw, dict): return {}, {}
    nested = all(isinstance(v, dict) for v in raw.values())
    if nested:
        by_venue: dict[str, dict[str, float]] = {}
        flat: dict[str, float] = {}
        for venue, token_map in raw.items():
            vmap: dict[str, float] = {}
            for token, amt in (token_map or {}).items():
                val = round(_safe_float(amt), 8)
                vmap[token] = val
                flat[token] = round(flat.get(token, 0.0) + val, 8)
            by_venue[venue] = vmap
        return flat, by_venue
    else:
        flat = {t: round(_safe_float(a), 8) for t, a in raw.items()}
        return flat, {}

@flask_app.post("/api/telemetry/push")
def api_telemetry_push():
    data = request.get_json(silent=True) or {}
    e = _maybe_require_hmac_tel(data)
    if e: return e
    agent_id = data.get("agent_id", "edge")
    raw_balances = data.get("balances") or {}
    flat, by_venue = _normalize_balances(raw_balances)
    _last_telemetry["agent_id"] = agent_id
    _last_telemetry["flat"] = flat
    _last_telemetry["by_venue"] = by_venue
    _last_telemetry["ts"] = int(time.time())
    if by_venue:
        venue_counts = {v: len(tokens) for v, tokens in by_venue.items()}
        log.info("📡 Telemetry from %s: venues=%s | flat_tokens=%d", agent_id, venue_counts, len(flat))
    else:
        preview = dict(list(flat.items())[:4])
        log.info("📡 Telemetry from %s: %s%s", agent_id, preview, " …" if len(flat) > 4 else "")
    return jsonify(ok=True, received=(len(by_venue) or len(flat))), 200

@flask_app.post("/api/telemetry/push_balances")
@flask_app.post("/api/edge/balances")
@flask_app.post("/bus/push_balances")
def api_telemetry_push_aliases():
    return api_telemetry_push()

@flask_app.get("/api/telemetry/last")
def api_telemetry_last():
    return jsonify(ok=True, **_last_telemetry), 200

@flask_app.get("/dash")
def dash():
    try: q = _queue_depth()
    except Exception: q = {}
    age = "-"
    if _last_telemetry.get("ts"):
        age = f"{int(time.time())-int(_last_telemetry['ts'])}s"
    html = f"""
    <html><head><meta charset="utf-8"><title>NovaTrade Dash</title>
    <style>
      body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto; margin:24px; }}
      .card {{ padding:16px; border:1px solid #e5e7eb; border-radius:12px; margin-bottom:12px; }}
      code {{ background:#f3f4f6; padding:2px 6px; border-radius:6px; }}
    </style></head><body>
      <h2>NovaTrade Bus</h2>
      <div class='card'><b>Telemetry age:</b> <code>{age}</code></div>
      <div class='card'><b>Queue</b><br>queued:{q.get('queued',0)} · leased:{q.get('leased',0)} · acked:{q.get('acked',0)} · failed:{q.get('failed',0)}</div>
      <div class='card'><b>Agent:</b> <code>{_last_telemetry.get('agent_id') or '-'}</code></div>
    </body></html>"""
    return html, 200, {"Content-Type":"text/html; charset=utf-8"}

# ============================================================================
# Command Bus (enqueue / pull / ack)
# ============================================================================
BUS_ROUTES = Blueprint("bus_routes", __name__, url_prefix="/api")

def _now_ts() -> int: return int(time.time())

@BUS_ROUTES.route("/intent/enqueue", methods=["POST"])
def intent_enqueue():
    body, err = _require_json()
    if err: return err
    if os.getenv("NOVA_KILL","").lower() in ("1","true","yes"): return jsonify(ok=False, error="bus_killed"), 503
    e = _maybe_require_hmac_ops(body)
    if e: return e
    required = ["agent_target","venue","symbol","side","amount"]
    missing = [k for k in required if not str(body.get(k,"")).strip()]
    if missing: return (f"missing fields: {', '.join(missing)}", 400)
    side = str(body["side"]).lower()
    if side not in ("buy","sell"): return ("side must be buy|sell", 400)
    try:
        amount = float(body["amount"])
        if amount <= 0: return ("amount must be > 0", 400)
    except Exception:
        return ("amount must be numeric", 400)
    cmd_id = body.get("id") or str(uuid.uuid4())
    payload = {
        "id": cmd_id,
        "ts": body.get("ts", _now_ts()),
        "source": body.get("source","operator"),
        "agent_target": body["agent_target"],
        "venue": str(body["venue"]).upper(),
        "symbol": str(body["symbol"]).upper(),
        "side": side,
        "amount": amount,
        "flags": body.get("flags", []),
    }
    try:
        _enqueue_command(cmd_id, payload)
        log.info("enqueue id=%s venue=%s symbol=%s side=%s amount=%s",
         cmd_id, payload["venue"], payload["symbol"], payload["side"], payload["amount"])
        send_telegram(f"✅ <b>Intent enqueued</b>\n<code>{json.dumps(payload,indent=2)}</code>")
        return jsonify({"ok": True, "id": cmd_id})
    except Exception as ex:
        send_telegram(f"⚠️ <b>Enqueue failed</b>\n{ex}")
        return (f"enqueue error: {ex}", 500)
    
    
@BUS_ROUTES.route("/ops/enqueue", methods=["POST"])
def ops_enqueue_alias():
    return intent_enqueue()

@BUS_ROUTES.route("/commands/pull", methods=["POST"])
def commands_pull():
    body, err = _require_json()
    if err: return err
    e = _maybe_require_hmac_pull(body)
    if e: return e
    agent_id = str(body.get("agent_id","")).strip() or "edge-primary"
    max_items = int(body.get("max", 5) or 5)
    lease_seconds = int(body.get("lease_seconds", 90) or 90)
    cmds = _pull_commands(agent_id, max_items=max_items, lease_seconds=lease_seconds)
    log.info("pull agent=%s count=%d lease=%ds", agent_id, len(cmds), lease_seconds)
    return jsonify({"ok": True, "commands": cmds})
        
@BUS_ROUTES.route("/commands/ack", methods=["POST"])
def commands_ack():
    body, err = _require_json()
    if err: return err
    e = _maybe_require_hmac_pull(body)
    if e: return e
    cmd_id  = str(body.get("command_id","")).strip()
    agent_id= str(body.get("agent_id","")).strip() or "edge-primary"
    status  = str(body.get("status","")).strip().lower() or "ok"
    detail  = body.get("detail", {})
    if not cmd_id: return ("command_id required", 400)
    try:
        _ack_command(cmd_id, agent_id, status, detail)
        if status == "ok":
            send_telegram(f"🧾 <b>ACK</b> {cmd_id} — <i>{status}</i>")
        else:
            send_telegram(f"🧾 <b>ACK</b> {cmd_id} — <i>{status}</i>\n<code>{json.dumps(detail,indent=2)}</code>")
        log.info("ack id=%s agent=%s status=%s", cmd_id, agent_id, status)
        return jsonify({"ok": True})
    except Exception as ex:
        return (f"ack error: {ex}", 500)
        

@BUS_ROUTES.route("/receipts/last", methods=["GET"])
def receipts_last():
    try:
        n = int(request.args.get("n", "10"))
    except Exception:
        n = 10
    try:
        rows = _last_receipts(n)
        return jsonify({"ok": True, "receipts": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@BUS_ROUTES.route("/health/summary", methods=["GET"])
def health_summary():
    try: q = _queue_depth()
    except Exception: q = {}
    age = "-"
    if _last_telemetry.get("ts"):
        age = f"{int(time.time())-int(_last_telemetry['ts'])}s"
    data = { 
        "ok": True,
        "service": os.getenv("SERVICE_NAME","bus"),
        "env": os.getenv("ENV","prod"),
        "queue": q,
        "telemetry_age": age,
        "agent": _last_telemetry.get("agent_id"),
     }
    include = request.args.get("include_receipts","0").lower() in ("1","true","yes")
    if include:
        try:
            data["receipts"] = _last_receipts(int(request.args.get("n","10")))
        except Exception as e:
            data["receipts_error"] = str(e)
    return jsonify(data)

flask_app.register_blueprint(BUS_ROUTES)

# ============================================================================
# Debug helpers
# ============================================================================
@flask_app.get("/api/debug/log")
def api_debug_log():
    log.info("debug-log: hello from bus")
    return jsonify(ok=True), 200

@flask_app.post("/api/debug/tg/send")
def api_debug_tg_send():
    # Optional protection with TELEGRAM_WEBHOOK_SECRET
    secret = os.getenv("TELEGRAM_WEBHOOK_SECRET","")
    if secret:
        got = request.args.get("secret") or request.headers.get("X-TG-Secret")
        if (got or "") != secret:
            return jsonify(ok=False, error="forbidden"), 403
    text = (request.get_json(silent=True) or {}).get("text") or "NovaTrade test ✅"
    send_telegram(text)
    return jsonify(ok=True), 200

@flask_app.errorhandler(404)
def _not_found(_e): return jsonify(error="not_found"), 404

@flask_app.errorhandler(405)
def _method_not_allowed(_e): return jsonify(error="method_not_allowed"), 405

@flask_app.errorhandler(500)
def _server_error(e):
    log.warning("Unhandled error: %s", e)
    return jsonify(error="internal_error"), 500

@flask_app.get("/api/debug/selftest")
def api_debug_selftest():
    try:
        test_id = f"selftest-{uuid.uuid4()}"
        payload = {"id": test_id, "ts": int(time.time()), "source": "selftest"}
        _enqueue_command(test_id, payload)
        q = _queue_depth()
        return jsonify(ok=True, test_id=test_id, queue=q, db=DB_PATH), 200
    except Exception as e:
        log.warning("selftest failed: %s", e)
        return jsonify(ok=False, error=str(e), db=DB_PATH), 200

# ============================================================================
# Receipts bridge (optional)
# ============================================================================
def _start_receipts_bridge():
    if os.environ.get("DISABLE_RECEIPTS_BRIDGE","").lower() in ("1","true","yes"):
        return
    try:
        import receipts_bridge  # user module providing run_once()
    except Exception as e:
        log.debug("receipts_bridge unavailable: %s", e)
        return
    def _loop():
        time.sleep(15)
        while True:
            try:
                receipts_bridge.run_once()  # type: ignore
            except Exception as err:
                log.info("receipts_bridge error: %s", err)
            time.sleep(300)
    threading.Thread(target=_loop, name="receipts-bridge", daemon=True).start()
    log.info("receipts_bridge scheduled every 5m")

_start_receipts_bridge()

# === NovaTrade lightweight scheduler (no Render Cron) =======================
import os, threading, time
from datetime import datetime, timedelta, timezone

def _tg_send(text: str) -> bool:
    """Minimal Telegram sender using existing BOT_TOKEN/TELEGRAM_CHAT_ID."""
    try:
        token = os.getenv("BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if not token or not chat_id:
            return False
        import requests
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
        return r.ok
    except Exception:
        return False

def _compose_daily_summary():
    """Calls our own /api/health/summary and composes a one-line report."""
    import requests, os
    bases = [
        os.getenv("RENDER_EXTERNAL_URL","").strip(),
        os.getenv("BUS_BASE_URL","").strip(),
        os.getenv("BASE_URL","").strip(),
        "http://127.0.0.1:10000",
        "http://localhost:10000",
    ]
    j = None
    for b in bases:
        if not b:
            continue
        try:
            r = requests.get(b.rstrip("/") + "/api/health/summary", timeout=8)
            if r.ok:
                j = r.json(); break
        except Exception:
            continue
    if not j:
        return "☀️ NovaTrade Daily Report\nhealth endpoint unavailable"
    q = j.get("queue", {})
    return (
        "☀️ NovaTrade Daily Report\n"
        f"service: {j.get('service','bus')} | agent: {j.get('agent','?')} | ok: {j.get('ok')}\n"
        f"queue: acked={q.get('acked',0)} failed={q.get('failed',0)} "
        f"leased={q.get('leased',0)} queued={q.get('queued',0)}\n"
        f"telemetry_age: {j.get('telemetry_age','n/a')}"
    )

def _run_sheets_mirror_once():
    """Best-effort call to your sheets mirror job if present."""
    try:
        import sheets_mirror  # your existing module
        sheets_mirror.main()
        return True
    except Exception as e:
        _tg_send(f"Sheets mirror failed: {e}")
        return False

def _seconds_until(hour: int, minute: int) -> int:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return max(1, int((target - now).total_seconds()))

def _start_nt_scheduler(app):
    """Start a daemon thread that does: mirror every X sec + daily TG summary."""
    mirror_iv = int(os.getenv("MIRROR_INTERVAL_SEC", "600"))     # default 10m
    daily_h   = int(os.getenv("HEALTH_UTC_HOUR", "13"))          # default 13:00 UTC
    daily_m   = int(os.getenv("HEALTH_UTC_MIN",  "0"))

    stop = threading.Event()

    def loop():
        next_mirror = time.time() + mirror_iv
        next_daily  = time.time() + _seconds_until(daily_h, daily_m)
        _tg_send("🕰️ NovaTrade scheduler started")
        while not stop.is_set():
            now = time.time()
            try:
                if now >= next_mirror:
                    _run_sheets_mirror_once()
                    next_mirror = now + mirror_iv
                # run PD mirror every 10m along with the receipts mirror
                try:
                    _pd.main()
                except Exception as e:
                    _tg_send_raw(f"PD mirror error: {e}")
                
                # once a day, compact receipts (2:15 UTC)
                if int(datetime.utcnow().strftime("%H%M")) == 215:
                    try:
                        _rc.compact_once()
                    except Exception as e:
                        _tg_send_raw(f"Compactor error: {e}")
                if now >= next_daily:
                    _tg_send(_compose_daily_summary())
                    next_daily = now + 24*3600
            except Exception:
                pass
            stop.wait(2.0)

    threading.Thread(target=loop, name="NovaScheduler", daemon=True).start()
    return stop

# Manual mirror trigger
@flask_app.route("/api/cron/mirror", methods=["POST"])
def _cron_mirror():
    ok = _run_sheets_mirror_once()
    return {"ok": bool(ok)}, (200 if ok else 500)

# kick it off after the app exists
try:
    _nt_sched_stop = _start_nt_scheduler(flask_app)
except Exception:
    pass
# =======================================================================

# ============================================================================
# ASGI adapter (uvicorn)
# ============================================================================
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore
