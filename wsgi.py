# wsgi.py — NovaTrade Bus (drop-in, single file)
# - Quiet logging (INFO by default)
# - Health: /, /healthz, /readyz, /api/health/summary
# - Telemetry: /api/telemetry/push (+ legacy aliases), /api/telemetry/last
# - Command Bus: /api/intent/enqueue, /api/commands/pull, /api/commands/ack  (HMAC-capable)
# - Durable SQLite outbox (WAL), leases & receipts (embedded here)
# - Optional Telegram notices (ENABLE_TELEGRAM=true)
# - Receipts bridge thread (if receipts_bridge.run_once present)
# - ASGI shim (WsgiToAsgi) for uvicorn

from __future__ import annotations
import os, logging, threading, time, json, uuid, hmac, hashlib, sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from flask import Flask, jsonify, request, Blueprint

# ============================================================================
# Logging
# ============================================================================
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bus")

# keep werkzeug quiet unless DEBUG
logging.getLogger("werkzeug").setLevel(logging.WARNING if LOG_LEVEL != "DEBUG" else logging.DEBUG)

# ============================================================================
# Flask app
# ============================================================================
flask_app = Flask(__name__)

# ============================================================================
# Embedded: Telegram helper (quiet, optional)
# ============================================================================
def send_telegram(text: str):
    if os.getenv("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text[:4000], "parse_mode": "HTML"},
            timeout=8,
        )
    except Exception as e:
        log.debug("telegram send degraded: %s", e)

# Try to mount a Telegram webhook at /tg if provided by the user, without ever crashing the Bus.
def _maybe_init_telegram(app: Flask) -> Optional[str]:
    if os.environ.get("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return None

    try:
        # We accept any of these export styles from telegram_webhook.py:
        #   tg_blueprint (Blueprint), telegram_bp (Blueprint), telegram_app (Flask), create_blueprint() (factory)
        import types
        import telegram_webhook as tgmod

        tg_bp = None
        if hasattr(tgmod, "tg_blueprint"):
            tg_bp = getattr(tgmod, "tg_blueprint")
        elif hasattr(tgmod, "telegram_bp"):
            tg_bp = getattr(tgmod, "telegram_bp")
        elif hasattr(tgmod, "create_blueprint") and callable(getattr(tgmod, "create_blueprint")):
            tg_bp = tgmod.create_blueprint()

        if tg_bp is not None:
            from flask import Blueprint
            if isinstance(tg_bp, Blueprint):
                app.register_blueprint(tg_bp, url_prefix="/tg")
                log.info("Telegram blueprint mounted at /tg")
            else:
                # Not a Blueprint — fall back to WSGI mount if it's a Flask app
                try:
                    from werkzeug.middleware.dispatcher import DispatcherMiddleware
                    # If this is a Flask() instance, it exposes .wsgi_app
                    subapp = getattr(tg_bp, "wsgi_app", None)
                    if subapp is None:
                        raise TypeError("telegram object is neither Blueprint nor Flask.wsgi_app")
                    app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {"/tg": subapp})
                    log.info("Telegram Flask app mounted at /tg")
                except Exception as e:
                    log.warning("Telegram mount degraded (not a Blueprint/Flask app): %s", e)
                    return str(e)

        # Optional: best-effort webhook setter
        if hasattr(tgmod, "set_telegram_webhook") and callable(getattr(tgmod, "set_telegram_webhook")):
            try:
                tgmod.set_telegram_webhook()
            except Exception as e:
                log.info("Telegram webhook setter degraded: %s", e)

        return None

    except Exception as e:
        # Never crash the Bus because of Telegram wiring
        log.warning("Telegram init failed: %s", e)
        return str(e)

telegram_status = _maybe_init_telegram(flask_app)

# ============================================================================
# Embedded: HMAC helpers
# ============================================================================
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "")

# --- HMAC flags (honor your env) ---
REQUIRE_HMAC_OPS = os.getenv("REQUIRE_HMAC_OPS", "0").lower() in ("1","true","yes")
REQUIRE_HMAC_PULL = os.getenv("REQUIRE_HMAC_PULL", "0").lower() in ("1","true","yes")
REQUIRE_HMAC_TELEMETRY = os.getenv("REQUIRE_HMAC_TELEMETRY", "0").lower() in ("1","true","yes")

# Allow a separate secret for telemetry if desired (falls back to OUTBOX_SECRET)
TELEMETRY_SECRET = os.getenv("TELEMETRY_SECRET", OUTBOX_SECRET)

def _canonical(body: dict) -> bytes:
    return json.dumps(body, separators=(",",":"), sort_keys=True).encode("utf-8")

def _hmac_sign(body: dict) -> str:
    if not OUTBOX_SECRET:
        return ""
    return hmac.new(OUTBOX_SECRET.encode("utf-8"), _canonical(body), hashlib.sha256).hexdigest()

def _hmac_verify(body: dict, provided_sig: str) -> bool:
    if not OUTBOX_SECRET:
        # dev mode (no secret) — accept
        return True
    try:
        expected = _hmac_sign(body)
        return hmac.compare_digest(expected, provided_sig or "")
    except Exception:
        return False

def _require_json():
    if not request.is_json:
        return None, ("invalid or missing JSON body", 400)
    try:
        return request.get_json(force=True, silent=False), None
    except Exception:
        return None, ("malformed JSON", 400)

def _hmac_verify_with(secret: str, body: dict, provided_sig: str) -> bool:
    if not secret:  # dev lenience
        return True
    try:
        expected = hmac.new(secret.encode("utf-8"),
                            json.dumps(body, separators=(",",":"), sort_keys=True).encode("utf-8"),
                            hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, provided_sig or "")
    except Exception:
        return False

def _maybe_require_hmac_ops(body):
    if REQUIRE_HMAC_OPS:
        sig = request.headers.get("X-NT-Sig","")
        if not _hmac_verify_with(OUTBOX_SECRET, body, sig):
            return ("invalid signature", 401)
    return None

def _maybe_require_hmac_pull(body):
    if REQUIRE_HMAC_PULL:
        sig = request.headers.get("X-NT-Sig","")
        if not _hmac_verify_with(OUTBOX_SECRET, body, sig):
            return ("invalid signature", 401)
    return None

def _maybe_require_hmac_tel(body):
    if REQUIRE_HMAC_TELEMETRY:
        sig = request.headers.get("X-NT-Sig","")
        if not _hmac_verify_with(TELEMETRY_SECRET, body, sig):
            return ("invalid signature", 401)
    return None

# ============================================================================
# Embedded: SQLite outbox store (WAL)
# ============================================================================
# --- SQLite outbox path ------------------------------------------------------
DB_PATH = os.getenv("OUTBOX_DB_PATH", "./outbox.sqlite")

# Ensure the directory exists (avoids "unable to open database file" 500s)
import os
try:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
except Exception as e:
    import logging
    logging.warning("Could not create DB directory for %s: %s", DB_PATH, e)

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',     -- queued|leased|acked|failed
  created_at TEXT NOT NULL,
  leased_at TEXT,
  lease_expires_at TEXT,
  agent_id TEXT
);
CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id TEXT NOT NULL,
  agent_id TEXT,
  status TEXT NOT NULL,                      -- ok|error|skipped
  detail TEXT,
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

def _init_db():
    conn = _connect()
    try:
        for stmt in SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
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
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO receipts(command_id, agent_id, status, detail, created_at) VALUES (?,?,?,?,?)",
            (cmd_id, agent_id, status.lower(), json.dumps(detail or {}, separators=(",",":")), now)
        )
        conn.execute("UPDATE commands SET status='acked' WHERE id=?", (cmd_id,))
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
# Health / basic endpoints
# ============================================================================
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info: Dict[str, Any] = {"ok": True, "web": "up"}
    info["telegram"] = {"status": "ok" if not telegram_status else "degraded", "reason": telegram_status}
    try:
        for name in ("VERSION", "version.txt", ".version"):
            p = os.path.join(os.getcwd(), name)
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as fh:
                    info["version"] = fh.read().strip()
                break
    except Exception as e:
        log.debug("version read failed: %s", e)
    info["db"] = os.environ.get("OUTBOX_DB_PATH", "unset")
    info["queue"] = _queue_depth()
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True, service="Bus", ready=True), 200

# ============================================================================
# Telemetry (Edge) — with legacy aliases
# ============================================================================
_last_telemetry: Dict[str, Any] = {"agent_id": None, "flat": {}, "by_venue": {}}

def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _normalize_balances(raw) -> tuple[dict, dict]:
    if not isinstance(raw, dict):
        return {}, {}
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

# ============================================================================
# Command Bus (enqueue / pull / ack)
# ============================================================================
BUS_ROUTES = Blueprint("bus_routes", __name__, url_prefix="/api")

def _now_ts() -> int:
    return int(time.time())

@BUS_ROUTES.route("/intent/enqueue", methods=["POST"])
def intent_enqueue():
    body, err = _require_json()
    if err: return err
    e = _maybe_require_hmac_ops(body)
    if e: return e

    # minimal validation
    required = ["agent_target","venue","symbol","side","amount"]
    missing = [k for k in required if not str(body.get(k,"")).strip()]
    if missing:
        return (f"missing fields: {', '.join(missing)}", 400)

    side = str(body["side"]).lower()
    if side not in ("buy","sell"):
        return ("side must be buy|sell", 400)
    try:
        amount = float(body["amount"])
        if amount <= 0:
            return ("amount must be > 0", 400)
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
        send_telegram(f"✅ <b>Intent enqueued</b>\n<code>{json.dumps(payload,indent=2)}</code>")
        return jsonify({"ok": True, "id": cmd_id})
    except Exception as ex:
        send_telegram(f"⚠️ <b>Enqueue failed</b>\n{ex}")
        return (f"enqueue error: {ex}", 500)

@BUS_ROUTES.route("/ops/enqueue", methods=["POST"])
def ops_enqueue_alias():
    # exact alias to maintain backward-compat with OPS_ENQUEUE_URL
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
    return jsonify({"ok": True, "commands": cmds})

@BUS_ROUTES.route("/commands/ack", methods=["POST"])
def commands_ack():
    body, err = _require_json()
    if err: return err
    e = _maybe_require_hmac_pull(body)
    if e: return e

    cmd_id = str(body.get("command_id","")).strip()
    agent_id = str(body.get("agent_id","")).strip() or "edge-primary"
    status = str(body.get("status","")).strip().lower() or "ok"
    detail = body.get("detail", {})

    if not cmd_id:
        return ("command_id required", 400)

    try:
        _ack_command(cmd_id, agent_id, status, detail)
        if status == "ok":
            send_telegram(f"🧾 <b>ACK</b> {cmd_id} — <i>{status}</i>")
        else:
            send_telegram(f"🧾 <b>ACK</b> {cmd_id} — <i>{status}</i>\n<code>{json.dumps(detail,indent=2)}</code>")
        return jsonify({"ok": True})
    except Exception as ex:
        return (f"ack error: {ex}", 500)

@BUS_ROUTES.route("/health/summary", methods=["GET"])
def health_summary():
    try:
        q = _queue_depth()
    except Exception:
        q = {}
    return jsonify({
        "ok": True,
        "service": os.getenv("SERVICE_NAME","bus"),
        "env": os.getenv("ENV","dev"),
        "queue": q,
    })

# mount blueprint
flask_app.register_blueprint(BUS_ROUTES)

# ============================================================================
# Minimal legacy endpoints kept for compatibility (noop/quiet)
# ============================================================================
@flask_app.post("/api/commands/pull")  # legacy shim (kept; will not be reached if blueprint registered first)
def _legacy_pull():
    log.debug("🪙 Edge poll → ok (legacy empty)")
    return jsonify(ok=True, commands=[]), 200

@flask_app.post("/api/commands/ack")   # legacy shim
def _legacy_ack():
    data = request.get_json(silent=True) or {}
    log.info("✅ ACK (legacy) from %s → %s (%s)",
             data.get("agent_id", "edge"),
             data.get("command_id", "?"),
             data.get("status", "ok"))
    return jsonify(ok=True), 200

@flask_app.post("/api/heartbeat")
def api_heartbeat():
    return jsonify(ok=True, service="Bus", alive=True), 200

# ============================================================================
# Error handlers
# ============================================================================
@flask_app.errorhandler(404)
def _not_found(_e):
    return jsonify(error="not_found"), 404

@flask_app.errorhandler(405)
def _method_not_allowed(_e):
    return jsonify(error="method_not_allowed"), 405

@flask_app.errorhandler(500)
def _server_error(e):
    log.warning("Unhandled error: %s", e)
    return jsonify(error="internal_error"), 500

@flask_app.get("/api/debug/selftest")
def api_debug_selftest():
    # Prove DB is usable and schema is present
    try:
        test_id = f"selftest-{uuid.uuid4()}"
        payload = {"id": test_id, "ts": int(time.time()), "source": "selftest"}
        _enqueue_command(test_id, payload)  # uses the same insert as /intent/enqueue
        q = _queue_depth()
        return jsonify(ok=True, test_id=test_id, queue=q, db=DB_PATH), 200
    except Exception as e:
        log.warning("selftest failed: %s", e)
        return jsonify(ok=False, error=str(e), db=DB_PATH), 500

# ============================================================================
# Receipts bridge (optional background loop)
# ============================================================================
def _start_receipts_bridge():
    if os.environ.get("DISABLE_RECEIPTS_BRIDGE", "").lower() in ("1", "true", "yes"):
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

# tiny banner to prove scheduler thread works (no cron here)
def _scheduler_banner():
    log.info("⏰ Scheduler thread active.")
threading.Thread(target=_scheduler_banner, daemon=True).start()

# ============================================================================
# ASGI adapter for uvicorn
# ============================================================================
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore
