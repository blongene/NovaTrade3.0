# wsgi.py — NovaTrade Bus (Phase 7A+: policy-wired, clean HMAC, dual kills, Telegram)
from __future__ import annotations
import os, json, hmac, hashlib, logging, threading, time, uuid, traceback
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
from flask import Flask, request, jsonify, Blueprint

# ========================= Logging =========================
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bus")
logging.getLogger("werkzeug").setLevel(logging.WARNING if LOG_LEVEL != "DEBUG" else logging.DEBUG)

# ========================= Flask ==========================
flask_app = Flask(__name__)

# ====================== Telegram ==========================
def _env_true(k: str) -> bool:
    return os.environ.get(k, "").lower() in ("1","true","yes","on")

ENABLE_TELEGRAM = _env_true("ENABLE_TELEGRAM")

def _bot_token() -> Optional[str]:
    # prefer BOT_TOKEN (your env), fallback to TELEGRAM_BOT_TOKEN
    return os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

_TELEGRAM_TOKEN = _bot_token()
_TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram(text: str):
    if not (ENABLE_TELEGRAM and _TELEGRAM_TOKEN and _TELEGRAM_CHAT):
        return
    try:
        import requests
        r = requests.post(
            f"https://api.telegram.org/bot{_TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": _TELEGRAM_CHAT, "text": text[:4000], "parse_mode": "HTML"},
            timeout=8
        )
        if not r.ok:
            log.warning("Telegram send failed: %s", r.text)
    except Exception as e:
        log.warning("Telegram degraded: %s", e)

# Try to mount your existing telegram_webhook module at /tg (optional)
try:
    import telegram_webhook as _tg
    if hasattr(_tg, "tg_blueprint"):
        flask_app.register_blueprint(_tg.tg_blueprint, url_prefix="/tg")
        log.info("Telegram blueprint mounted at /tg")
    if hasattr(_tg, "set_telegram_webhook"):
        try: _tg.set_telegram_webhook()
        except Exception as e: log.info("Telegram webhook setter degraded: %s", e)
except Exception as e:
    log.info("telegram_webhook not mounted: %s", e)

# Helpers to set/get webhook directly (diagnostics)
def _tg_api(path: str) -> str:
    tok = _bot_token()
    if not tok: raise RuntimeError("BOT_TOKEN missing")
    return f"https://api.telegram.org/bot{tok}/{path}"

def _guess_base_url() -> Optional[str]:
    base = os.getenv("TELEGRAM_WEBHOOK_BASE") or os.getenv("OPS_BASE_URL")
    if base: return base.rstrip("/")
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    return f"https://{host}".rstrip("/") if host else None

def _compute_webhook_url() -> Optional[str]:
    secret = os.getenv("TELEGRAM_WEBHOOK_SECRET")
    base = _guess_base_url()
    if not (secret and base): return None
    return f"{base}/tg/{secret}"

def _set_webhook_now() -> dict:
    import requests
    url = _compute_webhook_url()
    if not url:
        return {"ok": False, "reason": "missing TELEGRAM_WEBHOOK_SECRET or base url"}
    try:
        resp = requests.post(_tg_api("setWebhook"), json={"url": url}, timeout=10)
        data = resp.json() if resp.content else {}
        return {"ok": bool(data.get("ok")), "result": data}
    except Exception as e:
        return {"ok": False, "reason": f"{type(e).__name__}: {e}"}

def _get_webhook_info() -> dict:
    import requests
    try:
        r = requests.get(_tg_api("getWebhookInfo"), timeout=10)
        data = r.json() if r.content else {}
        return {"ok": bool(data.get("ok")), "result": data.get("result", data)}
    except Exception as e:
        return {"ok": False, "reason": f"{type(e).__name__}: {e}"}

def _maybe_autoset_webhook_on_boot():
    if _env_true("TELEGRAM_WEBHOOK_AUTOCONFIG") and _bot_token() and os.getenv("TELEGRAM_WEBHOOK_SECRET"):
        res = _set_webhook_now()
        try: log.info("telegram webhook autoconfig: %s", json.dumps(res, ensure_ascii=False))
        except Exception: log.info("telegram webhook autoconfig executed.")

try: _maybe_autoset_webhook_on_boot()
except Exception as _e: log.warning("telegram webhook autoconfig failed: %s", _e)

# ==================== HMAC / Security =====================
OUTBOX_SECRET          = os.getenv("OUTBOX_SECRET", "")
TELEMETRY_SECRET       = os.getenv("TELEMETRY_SECRET", OUTBOX_SECRET)
REQUIRE_HMAC_OPS       = _env_true("REQUIRE_HMAC_OPS")        # enqueue + ack
REQUIRE_HMAC_PULL      = _env_true("REQUIRE_HMAC_PULL")       # pull
REQUIRE_HMAC_TELEMETRY = _env_true("REQUIRE_HMAC_TELEMETRY")  # telemetry push

def _canonical(d: dict) -> bytes:
    return json.dumps(d, separators=(",",":"), sort_keys=True).encode("utf-8")

def _verify(secret: str, body: dict, sig: str) -> bool:
    if not secret: return True
    try:
        exp = hmac.new(secret.encode("utf-8"), _canonical(body), hashlib.sha256).hexdigest()
        return hmac.compare_digest(exp, sig or "")
    except Exception:
        return False

def _require_json():
    if not request.is_json: return None, (jsonify(ok=False, error="invalid_or_missing_json"), 400)
    try: return request.get_json(force=True, silent=False), None
    except Exception: return None, (jsonify(ok=False, error="malformed_json"), 400)

# ================= Kill Switches & Policy =================
CLOUD_HOLD     = _env_true("CLOUD_HOLD")
NOVA_KILL      = _env_true("NOVA_KILL")
ENABLE_POLICY  = _env_true("ENABLE_POLICY")
POLICY_ENFORCE = _env_true("POLICY_ENFORCE")
POLICY_PATH    = os.getenv("POLICY_PATH","policy.yaml")

class _PolicyState:
    def __init__(self):
        self.path = POLICY_PATH
        self.mtime = 0.0
        self.loaded = False
        self.engine = None
        self.load_error: Optional[str] = None

    def _mtime(self) -> float:
        try: return os.stat(self.path).st_mtime
        except FileNotFoundError: return 0.0

    def maybe_load(self, force: bool=False):
        if not ENABLE_POLICY:
            self.loaded, self.engine = False, None
            self.load_error = "policy disabled"
            return
        try:
            m = self._mtime()
            if force or (not self.loaded) or (m != self.mtime):
                import importlib
                pe = importlib.import_module("policy_engine")
                loader = getattr(pe, "load_policy", None)
                self.engine = loader(self.path) if callable(loader) else pe
                self.mtime = m
                self.loaded, self.load_error = True, None
                log.info("policy loaded: %s (mtime=%s)", self.path, self.mtime)
        except Exception as e:
            self.loaded, self.engine = False, None
            self.load_error = f"load error: {e}"
            log.warning("policy load error: %s", e)

    def evaluate(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """
        Expected responses (any of):
          { "ok": True/False, "reason": str, "patched_intent": {...}, "policy_id": str, "flags": [...] }
        Backward compat: { "allowed": bool, "patched": {...} }
        """
        self.maybe_load()
        if not (ENABLE_POLICY and self.loaded and self.engine):
            return {"ok": True, "reason": "policy disabled or not loaded"}
        try:
            eng = self.engine
            # prefer evaluate_intent()
            if hasattr(eng, "evaluate_intent"):
                res = eng.evaluate_intent(intent)
            elif hasattr(eng, "evaluate"):
                res = eng.evaluate(intent)
            elif hasattr(getattr(eng, "policy", None), "evaluate"):
                res = eng.policy.evaluate(intent)  # type: ignore
            else:
                return {"ok": True, "reason": "no evaluate function"}
            if not isinstance(res, dict):
                return {"ok": True, "reason": "policy returned non-dict"}
            # normalize legacy shape
            if "ok" not in res and "allowed" in res:
                res["ok"] = bool(res.get("allowed"))
            if "patched_intent" not in res and "patched" in res:
                res["patched_intent"] = res.get("patched") or {}
            return res
        except Exception as e:
            msg = f"policy exception: {e}"
            log.warning(msg)
            return {"ok": (not POLICY_ENFORCE), "reason": msg}

_policy = _PolicyState()
_policy.maybe_load(force=True)

def _policy_log(intent: dict, decision: dict):
    try:
        import policy_logger
        if hasattr(policy_logger, "log_decision"):
            policy_logger.log_decision(decision=decision, intent=intent, when=datetime.utcnow().isoformat())
            return
    except Exception:
        pass
    log.info("policy decision: %s", json.dumps({"decision": decision, "intent": intent}, separators=(",",":")))

# ================= Outbox (SQLite) ======================
DB_PATH = os.getenv("OUTBOX_DB_PATH", "./outbox.sqlite")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS commands (
  id TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  created_at TEXT NOT NULL,
  leased_at TEXT,
  lease_expires_at TEXT,
  agent_id TEXT
);
CREATE TABLE IF NOT EXISTS receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id TEXT NOT NULL,
  agent_id TEXT,
  status TEXT NOT NULL,
  detail TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status);
CREATE INDEX IF NOT EXISTS idx_commands_lease_exp ON commands(lease_expires_at);
"""

def _connect():
    import sqlite3
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def _init_db():
    conn = _connect()
    try:
        for stmt in [s.strip() for s in SCHEMA.strip().split(";") if s.strip()]:
            conn.execute(stmt)
    finally:
        conn.close()

_init_db()

def _enqueue_command(cmd_id: str, payload: Dict[str, Any]) -> None:
    now = datetime.utcnow().isoformat()
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO commands(id, payload, status, created_at) VALUES (?,?, 'queued', ?)",
            (cmd_id, json.dumps(payload, separators=(',',':')), now)
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
        conn.execute(
            """
            INSERT INTO receipts(command_id, agent_id, status, detail, created_at)
            VALUES (?,?,?,?,?)
            """,
            (cmd_id, agent_id, status.lower(), jdetail, now),
        )
        conn.execute("UPDATE commands SET status=? WHERE id=?", ("acked" if status=="ok" else "failed", cmd_id))
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
                payload = json.loads(detail) if detail else None
            except Exception:
                payload = {"raw": detail}
            out.append({"command_id": cid, "agent_id": aid, "status": st, "payload": payload, "ts": ts})
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

# ================= Root/Health/Dash =====================
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info: Dict[str, Any] = {"ok": True, "web": "up"}
    info["db"] = DB_PATH
    info["policy"] = {
        "enabled": ENABLE_POLICY,
        "enforce": POLICY_ENFORCE,
        "path": POLICY_PATH,
        "loaded": _policy.loaded,
        "error": _policy.load_error,
    }
    try: info["queue"] = _queue_depth()
    except Exception as e: info["queue_error"] = str(e)
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True, service="Bus", ready=True), 200

_last_tel: Dict[str, Any] = {"agent_id": None, "flat": {}, "by_venue": {}, "ts": 0}

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

# ================= Telemetry ============================
@flask_app.post("/api/telemetry/push")
def telemetry_push():
    body, err = _require_json()
    if err: return err
    if REQUIRE_HMAC_TELEMETRY and not _verify(TELEMETRY_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401
    agent_id = body.get("agent_id", "edge")
    flat, by_venue = _normalize_balances(body.get("balances") or {})
    _last_tel.update({"agent_id": agent_id, "flat": flat, "by_venue": by_venue, "ts": int(time.time())})
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
def telemetry_push_aliases():
    return telemetry_push()

@flask_app.get("/api/telemetry/last")
def telemetry_last():
    return jsonify(ok=True, **_last_tel), 200

# Simple HTML dash
@flask_app.get("/dash")
def dash():
    try: q = _queue_depth()
    except Exception: q = {}
    age = "-" if not _last_tel.get("ts") else f"{int(time.time())-int(_last_tel['ts'])}s"
    venues_line = ", ".join(f"{v}:{len(t)}" for v,t in _last_tel.get("by_venue",{}).items()) or "—"
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
      <div class='card'><b>Agent:</b> <code>{_last_tel.get('agent_id') or '-'}</code></div>
      <div class='card'><b>Policy:</b> <code>{"ENABLED" if ENABLE_POLICY else "DISABLED"} / {"ENFORCE" if POLICY_ENFORCE else "WARN"}</code></div>
    </body></html>"""
    return html, 200, {"Content-Type":"text/html; charset=utf-8"}

# ================= Bus API (enqueue/pull/ack) ===========
BUS = Blueprint("bus", __name__, url_prefix="/api")

def _now_ts() -> int: return int(time.time())

@BUS.route("/intent/enqueue", methods=["POST"])
def intent_enqueue():
    body, err = _require_json()
    if err: return err
    if NOVA_KILL or CLOUD_HOLD:
        return jsonify(ok=False, error="bus_killed"), 503
    if REQUIRE_HMAC_OPS and not _verify(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401

    # schema
    req = ["agent_target","venue","symbol","side","amount"]
    missing = [k for k in req if not str(body.get(k,"")).strip()]
    if missing: return jsonify(ok=False, error=f"missing: {', '.join(missing)}"), 400
    side = str(body["side"]).lower()
    if side not in ("buy","sell"): return jsonify(ok=False, error="side must be buy|sell"), 400
    try:
        amount = float(body["amount"])
        if amount <= 0: return jsonify(ok=False, error="amount must be > 0"), 400
    except Exception:
        return jsonify(ok=False, error="amount must be numeric"), 400

    intent = {
        "id": body.get("id") or str(uuid.uuid4()),
        "ts": body.get("ts", _now_ts()),
        "source": body.get("source","operator"),
        "agent_target": body["agent_target"],
        "venue": str(body["venue"]).upper(),
        "symbol": str(body["symbol"]).upper(),
        "side": side,
        "amount": amount,
        "flags": body.get("flags", []),
    }

    decision = {"ok": True, "reason": "no policy", "patched_intent": {}, "flags": []}
    if ENABLE_POLICY:
        try:
            decision = _policy.evaluate(intent)
        except Exception as ex:
            log.warning("policy evaluation exception: %s", ex)
    patched = decision.get("patched_intent") or decision.get("patched") or {}
    if patched:
        intent.update(patched)
    pol_flags = list(decision.get("flags") or [])
    if pol_flags:
        intent["flags"] = sorted(set(list(intent.get("flags", [])) + pol_flags))

    if ENABLE_POLICY and not decision.get("ok", True):
        _policy_log(intent, decision)
        if POLICY_ENFORCE:
            try: send_telegram(f"❌ <b>Policy blocked</b>\n<code>{json.dumps(intent,indent=2)}</code>\n<i>{decision.get('reason','')}</i>")
            except Exception: pass
            return jsonify(ok=False, policy="blocked", reason=decision.get("reason","")), 403
        else:
            # warn but proceed
            intent.setdefault("flags", []).append("policy_warn")
            try: send_telegram(f"⚠️ <b>Policy warning</b>\n<code>{json.dumps(intent,indent=2)}</code>\n<i>{decision.get('reason','')}</i>")
            except Exception: pass

    try:
        _enqueue_command(intent["id"], intent)
        log.info("enqueue id=%s venue=%s symbol=%s side=%s amount=%s", intent["id"], intent["venue"], intent["symbol"], intent["side"], intent["amount"])
        try: send_telegram(f"✅ <b>Intent enqueued</b>\n<code>{json.dumps(intent,indent=2)}</code>")
        except Exception: pass
        return jsonify(ok=True, id=intent["id"], decision=decision), 200
    except Exception as ex:
        try: send_telegram(f"⚠️ <b>Enqueue failed</b>\n{ex}")
        except Exception: pass
        return jsonify(ok=False, error=f"enqueue error: {ex}"), 500

@BUS.route("/ops/enqueue", methods=["POST"])
def ops_enqueue_alias():
    return intent_enqueue()

@BUS.route("/commands/pull", methods=["POST"])
def commands_pull():
    body, err = _require_json()
    if err: return err
    if REQUIRE_HMAC_PULL and not _verify(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401
    agent_id = (body or {}).get("agent_id") or "edge-primary"
    max_items = int((body or {}).get("max", 5) or 5)
    lease_seconds = int((body or {}).get("lease_seconds", 90) or 90)
    cmds = _pull_commands(agent_id, max_items=max_items, lease_seconds=lease_seconds)
    log.info("pull agent=%s count=%d lease=%ds", agent_id, len(cmds), lease_seconds)
    return jsonify(ok=True, commands=cmds), 200

@BUS.route("/commands/ack", methods=["POST"])
def commands_ack():
    body, err = _require_json()
    if err: return err
    # ACK is an operator-side action → use OPS HMAC
    if REQUIRE_HMAC_OPS and not _verify(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401
    cmd_id   = str(body.get("command_id","")).strip()
    agent_id = str(body.get("agent_id","")).strip() or "edge-primary"
    status   = str(body.get("status","ok")).strip().lower() or "ok"
    detail   = body.get("detail", {})
    if not cmd_id: return jsonify(ok=False, error="command_id required"), 400
    try:
        _ack_command(cmd_id, agent_id, status, detail)
        if status == "ok":
            try: send_telegram(f"🧾 <b>ACK</b> {cmd_id} — <i>{status}</i>")
            except Exception: pass
        else:
            try: send_telegram(f"🧾 <b>ACK</b> {cmd_id} — <i>{status}</i>\n<code>{json.dumps(detail,indent=2)}</code>")
            except Exception: pass
        log.info("ack id=%s agent=%s status=%s", cmd_id, agent_id, status)
        return jsonify(ok=True), 200
    except Exception as ex:
        return jsonify(ok=False, error=f"ack error: {ex}"), 500

@BUS.route("/receipts/last", methods=["GET"])
def receipts_last():
    try:
        n = int(request.args.get("n","10"))
    except Exception:
        n = 10
    try:
        rows = _last_receipts(n)
        return jsonify(ok=True, receipts=rows), 200
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@BUS.route("/health/summary", methods=["GET"])
def health_summary():
    try: q = _queue_depth()
    except Exception: q = {}
    age = "-" if not _last_tel.get("ts") else f"{int(time.time())-int(_last_tel['ts'])}s"
    data = {
        "ok": True,
        "service": os.getenv("SERVICE_NAME","bus"),
        "env": os.getenv("ENV","prod"),
        "queue": q,
        "telemetry_age": age,
        "agent": _last_tel.get("agent_id"),
        "policy": {
            "enabled": ENABLE_POLICY,
            "enforce": POLICY_ENFORCE,
            "path": POLICY_PATH,
            "loaded": _policy.loaded,
            "error": _policy.load_error,
        }
     }
    if (request.args.get("include_receipts","0").lower() in ("1","true","yes")):
        try: data["receipts"] = _last_receipts(int(request.args.get("n","10")))
        except Exception as e: data["receipts_error"] = str(e)
    return jsonify(data), 200

@BUS.route("/policy/reload", methods=["POST"])
def policy_reload():
    _policy.maybe_load(force=True)
    return jsonify(ok=True, enabled=ENABLE_POLICY, enforce=POLICY_ENFORCE, path=POLICY_PATH,
                   loaded=_policy.loaded, error=_policy.load_error), 200

@BUS.route("/policy/evaluate", methods=["POST"])
def policy_evaluate():
    body, err = _require_json()
    if err: return err
    decision = _policy.evaluate(body or {})
    return jsonify(ok=True, decision=decision), 200

flask_app.register_blueprint(BUS)

# ============== Debug & Telegram diag ====================
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

@flask_app.get("/api/debug/tg/webhook_info")
def api_tg_webhook_info():
    secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
    got = request.args.get("secret") or request.headers.get("X-TG-Secret")
    if secret and (got or "") != secret:
        return jsonify(ok=False, error="forbidden"), 403
    return jsonify(_get_webhook_info()), 200

@flask_app.post("/api/debug/tg/set_webhook")
def api_tg_set_webhook():
    secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
    got = request.args.get("secret") or request.headers.get("X-TG-Secret")
    if secret and (got or "") != secret:
        return jsonify(ok=False, error="forbidden"), 403
    res = _set_webhook_now()
    return jsonify(res), (200 if res.get("ok") else 400)

@flask_app.errorhandler(404)
def _not_found(_e): return jsonify(error="not_found"), 404

@flask_app.errorhandler(405)
def _method_not_allowed(_e): return jsonify(error="method_not_allowed"), 405

@flask_app.errorhandler(500)
def _server_error(e):
    log.warning("Unhandled error: %s", e)
    return jsonify(error="internal_error"), 500

# ============== Background: policy watchdog =============
def _policy_watchdog():
    while True:
        try: _policy.maybe_load()
        except Exception as e: log.debug("policy watchdog err: %s", e)
        time.sleep(10)

if ENABLE_POLICY:
    threading.Thread(target=_policy_watchdog, name="policy-watchdog", daemon=True).start()
    log.info("Policy watchdog started.")

# ============== Daily report (optional) ==================
DAILY_ENABLED   = _env_true("DAILY_ENABLED") or _env_true("ENABLE_TELEGRAM")
DAILY_UTC_HOUR  = int(os.getenv("DAILY_UTC_HOUR","9"))
DAILY_UTC_MIN   = int(os.getenv("DAILY_UTC_MIN","0"))

def _compose_daily() -> str:
    q = {}
    try: q = _queue_depth()
    except Exception: pass
    age_s = "-" if not _last_tel.get("ts") else f"{int(time.time())-int(_last_tel['ts'])}s"
    venues_line = ", ".join(f"{v}:{len(t)}" for v,t in _last_tel.get("by_venue",{}).items()) or "—"
    msg = (
        "☀️ <b>NovaTrade Daily Report</b>\n"
        f"as of {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        "<b>Heartbeats</b>\n"
        f"• {_last_tel.get('agent_id') or 'edge-primary'}: last {age_s} ago\n\n"
        "<b>Queue</b>\n"
        f"• queued:{q.get('queued',0)} leased:{q.get('leased',0)} acked:{q.get('acked',0)} failed:{q.get('failed',0)}\n\n"
        "<b>Balances (venues → tokenCount)</b>\n"
        f"• {venues_line}\n"
        f"Mode: <code>{os.getenv('EDGE_MODE','live')}</code>"
    )
    return msg

def _sleep_until(hour:int, minute:int):
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now: target = target + timedelta(days=1)
        time.sleep((target - now).total_seconds())
        yield

def _start_daily():
    if not (DAILY_ENABLED and ENABLE_TELEGRAM and _TELEGRAM_TOKEN and _TELEGRAM_CHAT):
        return
    def _loop():
        for _ in _sleep_until(DAILY_UTC_HOUR, DAILY_UTC_MIN):
            try: send_telegram(_compose_daily())
            except Exception as e: log.debug("daily send degraded: %s", e)
    threading.Thread(target=_loop, name="daily-report", daemon=True).start()
    log.info("Daily report scheduled for %02d:%02d UTC", DAILY_UTC_HOUR, DAILY_UTC_MIN)

_start_daily()
threading.Thread(target=lambda: log.info("⏰ Scheduler thread active."), daemon=True).start()

# ================= ASGI adapter =========================
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore
