# wsgi.py — NovaTrade Bus (Phase 7A: policy wired with telemetry context)
from __future__ import annotations
import os, json, hmac, hashlib, logging, threading, time, uuid
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
from flask import Flask, request, jsonify, Blueprint

# ========== Logging ==========
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bus")
logging.getLogger("werkzeug").setLevel(logging.WARNING if LOG_LEVEL != "DEBUG" else logging.DEBUG)

# ========== Flask ==========
flask_app = Flask(__name__)

# ========== Flags / helpers ==========
def _env_true(k: str) -> bool:
    return os.environ.get(k, "").lower() in ("1","true","yes","on")

def _canonical(d: dict) -> bytes:
    return json.dumps(d, separators=(",",":"), sort_keys=True).encode("utf-8")

# ========== Telegram (quiet/optional) ==========
ENABLE_TELEGRAM = _env_true("ENABLE_TELEGRAM")
def _bot_token() -> Optional[str]:
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

# Optional webhook blueprint
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

# ========== HMAC ==========
OUTBOX_SECRET          = os.getenv("OUTBOX_SECRET", "")
TELEMETRY_SECRET       = os.getenv("TELEMETRY_SECRET", OUTBOX_SECRET)
REQUIRE_HMAC_OPS       = _env_true("REQUIRE_HMAC_OPS")        # enqueue + ack
REQUIRE_HMAC_PULL      = _env_true("REQUIRE_HMAC_PULL")       # pull
REQUIRE_HMAC_TELEMETRY = _env_true("REQUIRE_HMAC_TELEMETRY")  # telemetry push

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

# ========== Kill switches & Policy flags ==========
CLOUD_HOLD     = _env_true("CLOUD_HOLD")
NOVA_KILL      = _env_true("NOVA_KILL")
ENABLE_POLICY  = _env_true("ENABLE_POLICY")
POLICY_ENFORCE = _env_true("POLICY_ENFORCE")
POLICY_PATH    = os.getenv("POLICY_PATH","policy.yaml")
from collections import deque
LAST_DECISIONS = deque(maxlen=5)
COOLDOWN_MINUTES = int(os.getenv("POLICY_COOLDOWN_MINUTES", "30"))
_last_intent_at = {}  # key: (venue,symbol,side) -> epoch seconds
_policy_overrides = {"ttl_expiry": 0}

# ========== Policy loader (with context) ==========
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

    def evaluate_intent(self, intent: Dict[str, Any], context: Optional[dict]=None) -> Dict[str, Any]:
        self.maybe_load()
        if not (ENABLE_POLICY and self.loaded and self.engine):
            return {"ok": True, "reason": "policy disabled or not loaded", "patched_intent": {}, "flags": []}
        try:
            eng = self.engine
            # prefer modern signature with context
            if hasattr(eng, "evaluate_intent"):
                return eng.evaluate_intent(intent, context=context)  # type: ignore
            # back-compat fallbacks
            if hasattr(eng, "evaluate"):
                return eng.evaluate(intent)  # type: ignore
            if hasattr(getattr(eng, "policy", None), "evaluate"):
                return eng.policy.evaluate(intent)  # type: ignore
            return {"ok": True, "reason": "no evaluate function", "patched_intent": {}, "flags": []}
        except Exception as e:
            msg = f"policy exception: {e}"
            log.warning(msg)
            return {"ok": (not POLICY_ENFORCE), "reason": msg, "patched_intent": {}, "flags": ["policy_exception"]}

_policy = _PolicyState()
_policy.maybe_load(force=True)

# Optional structured logging to sheet / local JSONL
def _policy_log(intent: dict, decision: dict):
    try:
        import policy_logger
        if hasattr(policy_logger, "log_decision"):
            policy_logger.log_decision(decision=decision, intent=intent, when=datetime.utcnow().isoformat())
            return
    except Exception:
        pass
    # fallback: compact log
    try:
        log.info("policy decision: %s", json.dumps({"intent": intent, "decision": decision}, separators=(",",":")))
    except Exception:
        log.info("policy decision (non-json-serializable)")

@flask_app.get("/api/policy/config")
def policy_config():
    try:
        eng = _policy.engine
        cfg = dict(getattr(eng, "cfg", {}) or {})
        # apply live overrides if not expired
        now = time.time()
        if _policy_overrides.get("ttl_expiry", 0) > now:
            for k, v in (_policy_overrides.get("values") or {}).items():
                cfg[k] = v
        return jsonify(ok=True, config=cfg), 200
    except Exception as e:
        return jsonify(ok=False, error=str(e)), 500

@flask_app.post("/api/policy/override")
def policy_override():
    body, err = _require_json()
    if err: return err
    if REQUIRE_HMAC_OPS and not _verify(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401
    values = body if isinstance(body, dict) else {}
    ttl = int(values.pop("ttl_sec", 3600) or 3600)
    _policy_overrides["values"] = values
    _policy_overrides["ttl_expiry"] = time.time() + ttl
    return jsonify(ok=True, applied=values, ttl_sec=ttl), 200

# ========== Outbox (SQLite, inline) ==========
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
import sqlite3
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn
def _init_db():
    with _connect() as c:
        for stmt in [s.strip() for s in SCHEMA.strip().split(";") if s.strip()]:
            c.execute(stmt)
_init_db()
def _enqueue_command(cmd_id: str, payload: Dict[str, Any]) -> None:
    now = datetime.utcnow().isoformat()
    with _connect() as c:
        c.execute("INSERT INTO commands(id, payload, status, created_at) VALUES (?,?, 'queued', ?)",
                  (cmd_id, json.dumps(payload, separators=(",",":")), now))
def _pull_commands(agent_id: str, max_items: int = 10, lease_seconds: int = 90) -> List[Dict]:
    now = datetime.utcnow()
    with _connect() as c:
        rows = c.execute(
            """SELECT id, payload FROM commands
               WHERE status IN ('queued','leased')
                 AND (lease_expires_at IS NULL OR lease_expires_at < ?)
               ORDER BY created_at ASC LIMIT ?""",
            (now.isoformat(), max_items)
        ).fetchall()
        if not rows: return []
        ids = [r[0] for r in rows]
        qmarks = ",".join("?" for _ in ids)
        c.execute(
            f"""UPDATE commands
                    SET status='leased', leased_at=?, lease_expires_at=?, agent_id=?
                WHERE id IN ({qmarks})""",
            (now.isoformat(), (now+timedelta(seconds=lease_seconds)).isoformat(), agent_id, *ids)
        )
        return [{"id": rid, "payload": json.loads(pay)} for rid, pay in rows]
def _ack_command(cmd_id: str, agent_id: str, status: str, detail: Optional[Dict] = None) -> None:
    now = datetime.utcnow().isoformat()
    with _connect() as c:
        c.execute("""INSERT INTO receipts(command_id, agent_id, status, detail, created_at)
                     VALUES (?,?,?,?,?)""", (cmd_id, agent_id, status.lower(),
                                             json.dumps(detail or {}, separators=(",",":")), now))
        c.execute("UPDATE commands SET status=? WHERE id=?", ("acked" if status=="ok" else "failed", cmd_id))
def _last_receipts(n: int = 10):
    with _connect() as c:
        rows = c.execute("""SELECT command_id, agent_id, status, detail, created_at
                            FROM receipts ORDER BY created_at DESC LIMIT ?""", (int(n),)).fetchall()
    out=[]
    for cid, aid, st, detail, ts in rows:
        try: payload = json.loads(detail) if detail else None
        except Exception: payload = {"raw": detail}
        out.append({"command_id": cid, "agent_id": aid, "status": st, "payload": payload, "ts": ts})
    return out
def _queue_depth() -> Dict[str,int]:
    with _connect() as c:
        out={}
        for st in ("queued","leased","acked","failed"):
            out[st] = int(c.execute("SELECT COUNT(1) FROM commands WHERE status=?", (st,)).fetchone()[0])
    return out

# ========== Health/root ==========
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info = {"ok": True, "web":"up", "db": DB_PATH}
    info["policy"] = {"enabled": ENABLE_POLICY, "enforce": POLICY_ENFORCE,
                      "path": POLICY_PATH, "loaded": _policy.loaded, "error": _policy.load_error}
    try: info["queue"] = _queue_depth()
    except Exception as e: info["queue_error"] = str(e)
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True), 200

# ========== Telemetry ==========
_last_tel: Dict[str, Any] = {"agent_id": None, "flat": {}, "by_venue": {}, "ts": 0}

def _safe_float(x) -> float:
    try: return float(x)
    except Exception: return 0.0

def _normalize_balances(raw) -> Tuple[dict, dict]:
    if not isinstance(raw, dict): return {}, {}
    nested = all(isinstance(v, dict) for v in raw.values())
    if nested:
        by_venue, flat = {}, {}
        for venue, token_map in raw.items():
            vmap={}
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
def telemetry_push():
    body, err = _require_json()
    if err: return err
    if REQUIRE_HMAC_TELEMETRY and not _verify(TELEMETRY_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401
    agent_id = (body or {}).get("agent_id") or "edge"
    flat, by_venue = _normalize_balances((body or {}).get("balances") or {})
    _last_tel.update({"agent_id": agent_id, "flat": flat, "by_venue": by_venue, "ts": int(time.time())})
    venues_line = ", ".join(f"{v}:{len(t)}" for v,t in by_venue.items()) or "—"
    log.info("📡 Telemetry from %s | venues=%s | flat_tokens=%d", agent_id, venues_line, len(flat))
    return jsonify(ok=True, received=(len(by_venue) or len(flat))), 200

@flask_app.post("/api/telemetry/push_balances")
@flask_app.post("/api/edge/balances")
@flask_app.post("/bus/push_balances")
def telemetry_push_aliases():
    return telemetry_push()

@flask_app.get("/api/telemetry/last")
def telemetry_last():
    age = (time.time() - _last_tel["ts"]) if _last_tel["ts"] else None
    return jsonify(ok=True, **_last_tel, age_sec=age), 200

# ========== Dash ==========
@flask_app.get("/dash")
def dash():
    try: q = _queue_depth()
    except Exception: q = {}
    age = "-" if not _last_tel.get("ts") else f"{int(time.time())-int(_last_tel['ts'])}s"
    html = f"""<html><head><meta charset="utf-8"><title>NovaTrade Dash</title>
    <style>body{{font-family:system-ui;margin:24px}}.card{{padding:16px;border:1px solid #e5e7eb;border-radius:12px;margin-bottom:12px}}</style>
    </head><body>
      <h2>NovaTrade Bus</h2>
      <div class='card'><b>Telemetry age:</b> {age}</div>
      <div class='card'><b>Queue</b> queued:{q.get('queued',0)} leased:{q.get('leased',0)} acked:{q.get('acked',0)} failed:{q.get('failed',0)}</div>
      <div class='card'><b>Agent:</b> {_last_tel.get('agent_id') or '-'}</div>
      <div class='card'><b>Policy:</b> {"ENABLED" if ENABLE_POLICY else "DISABLED"} / {"ENFORCE" if POLICY_ENFORCE else "WARN"}</div>
    </body></html>"""
    return html, 200, {"Content-Type":"text/html; charset=utf-8"}

# ========== Bus API ==========
BUS = Blueprint("bus", __name__, url_prefix="/api")

def _uniq_extend(dst, add):
    if not isinstance(dst, list): dst = []
    if not isinstance(add, list): add = [add] if add else []
    seen, out = set(), []
    for x in dst + add:
        if x is None: continue
        if x in seen: continue
        seen.add(x); out.append(x)
    return out

@BUS.route("/intent/enqueue", methods=["POST"])
def intent_enqueue():
    body, err = _require_json()
    if err:
        return err

    if NOVA_KILL or CLOUD_HOLD:
        return jsonify(ok=False, error="bus_killed"), 503

    if REQUIRE_HMAC_OPS and not _verify(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401

    # minimal schema
    req = ["agent_target","symbol","side","amount"]
    missing = [k for k in req if not str(body.get(k,"")).strip()]
    if missing:
        return jsonify(ok=False, error=f"missing: {', '.join(missing)}"), 400

    side = str(body["side"]).lower()
    if side not in ("buy","sell"):
        return jsonify(ok=False, error="side must be buy|sell"), 400

    try:
        amount = float(body["amount"])
        if amount <= 0:
            return jsonify(ok=False, error="amount must be > 0"), 400
    except Exception:
        return jsonify(ok=False, error="amount must be numeric"), 400

    intent = {
        "id": body.get("id") or str(uuid.uuid4()),
        "ts": body.get("ts", int(time.time())),
        "source": body.get("source","operator"),
        "agent_target": body["agent_target"],
        "venue": str(body.get("venue","") or "").upper(),   # optional; router may override
        "symbol": str(body["symbol"]).upper(),
        "side": side,
        "amount": amount,
        "flags": list(body.get("flags", [])),
        # optional hints:
        "price_usd": body.get("price_usd"),
        "notional_usd": body.get("notional_usd"),
        "quote_reserve_usd": body.get("quote_reserve_usd"),
    }

    # --- cooldown gate (anti-thrash) -----------------------------------------
    try:
        effective_cfg = dict(getattr(_policy.engine, "cfg", {}) or {})
        now = time.time()
        cd_min = int(os.getenv(
            "POLICY_COOLDOWN_MINUTES",
            str(effective_cfg.get("cool_off_minutes_after_trade", 30))
        ))
        if cd_min:
            key = (intent.get("venue"), intent["symbol"], intent["side"])
            last = _last_intent_at.get(key, 0)
            if (now - last) < cd_min * 60:
                remain = int(cd_min*60 - (now - last))
                decision = {"ok": False, "reason": f"cooldown active ({remain}s left)", "flags": ["cooldown"], "patched_intent": {}}
                LAST_DECISIONS.append({"intent": intent, "decision": decision, "ts": int(now)})
                return jsonify(ok=False, policy="blocked", reason=decision["reason"], decision=decision), 403
    except Exception as e:
        log.info("cooldown check degraded: %s", e)

    # --- router: choose best venue using telemetry + policy -------------------
    try:
        import router
        # take a copy of live policy cfg (with any active overrides surfaced by /api/policy/config)
        try:
            policy_cfg = dict(getattr(_policy.engine, "cfg", {}) or {})
        except Exception:
            policy_cfg = {}
        route_res = router.choose_venue(intent, _last_tel or {}, policy_cfg if isinstance(policy_cfg, dict) else {})
        if route_res.get("ok"):
            intent.update(route_res.get("patched_intent") or {})
            intent.setdefault("flags", []).extend(route_res.get("flags") or [])
            try:
                _last_intent_at[(intent.get("venue"), intent["symbol"], intent["side"])] = time.time()
            except Exception:
                pass
        else:
            LAST_DECISIONS.append({"intent": intent, "decision": route_res, "ts": int(time.time())})
            return jsonify(ok=False, policy="blocked", reason=route_res.get("reason","routing_failed"), decision=route_res), 403
    except Exception as e:
        # router failure should not crash; fall back to whatever was provided
        log.info("router degraded: %s", e)

    # --- policy evaluation with telemetry context ----------------------------
    try:
        context = {"telemetry": _last_tel}
        decision = _policy.evaluate_intent(intent, context=context)
    except Exception as e:
        msg = f"policy exception: {e}"
        log.warning(msg)
        decision = {"ok": (not POLICY_ENFORCE), "reason": msg, "patched_intent": {}, "flags": ["policy_exception"]}

    _policy_log(intent, decision)
    try:
        LAST_DECISIONS.append({"intent": intent, "decision": decision, "ts": int(time.time())})
    except Exception:
        pass

    # enforce policy if not ok
    if not decision.get("ok", True) and POLICY_ENFORCE:
        reason = decision.get("reason","policy_denied")
        send_telegram(f"❌ Policy blocked\n<code>{json.dumps(intent,indent=2)}</code>\n<i>{reason}</i>")
        return jsonify(ok=False, policy="blocked", reason=reason, decision=decision), 403

    # apply patches, if any
    patched = decision.get("patched_intent") or decision.get("patched") or {}
    if patched:
        intent.update(patched)

    # enqueue
    _enqueue_command(intent["id"], intent)
    log.info("enqueue id=%s venue=%s symbol=%s side=%s amount=%s",
             intent["id"], intent.get("venue"), intent["symbol"], intent["side"], intent["amount"])
    send_telegram(f"✅ Intent enqueued\n<code>{json.dumps(intent,indent=2)}</code>")
    return jsonify(ok=True, id=intent["id"], decision=decision), 200

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
    max_items = int((body or {}).get("max_items", 10))
    lease_ttl = int((body or {}).get("lease_ttl_sec", 90))
    cmds = _pull_commands(agent_id, max_items=max_items, lease_seconds=lease_ttl)
    if cmds: log.info("pull: leased %s cmds to %s", len(cmds), agent_id)
    return jsonify(ok=True, commands=cmds), 200

@BUS.route("/commands/ack", methods=["POST"])
def commands_ack():
    body, err = _require_json()
    if err: return err
    if REQUIRE_HMAC_OPS and not _verify(OUTBOX_SECRET, body, request.headers.get("X-NT-Sig","")):
        return jsonify(ok=False, error="invalid_signature"), 401
    cmd_id   = str(body.get("command_id","")).strip()
    agent_id = str(body.get("agent_id","")).strip() or "edge-primary"
    status   = str(body.get("status","ok")).strip().lower() or "ok"
    detail   = body.get("detail", {})
    if not cmd_id: return jsonify(ok=False, error="command_id required"), 400
    _ack_command(cmd_id, agent_id, status, detail)
    send_telegram(f"🧾 ACK <b>{status}</b>\n<b>id</b> {cmd_id}\n<b>by</b> {agent_id}")
    log.info("ack id=%s agent=%s status=%s", cmd_id, agent_id, status)
    return jsonify(ok=True), 200

@BUS.route("/receipts/last", methods=["GET"])
def receipts_last():
    try: n = int(request.args.get("n","10"))
    except Exception: n = 10
    try: rows = _last_receipts(n)
    except Exception as e: return jsonify(ok=False, error=str(e)), 500
    return jsonify(ok=True, receipts=rows), 200

@BUS.route("/health/summary", methods=["GET"])
def health_summary():
    now = time.time()

    try:
        q = _queue_depth()
    except Exception:
        q = {}

    tel = _last_tel or {}
    ts = tel.get("ts") or 0
    age_sec = (int(now - int(ts)) if ts else None)
    age_str = (f"{age_sec}s" if age_sec is not None else "unknown")

    venues_ct = len((tel.get("by_venue") or {}))
    flat_tokens_ct = len((tel.get("flat") or {}))

    # live override status (safe even if _policy_overrides not present)
    try:
        ov_expiry = _policy_overrides.get("ttl_expiry", 0)
        ov_active = bool(ov_expiry and ov_expiry > now)
        ov_vals = (_policy_overrides.get("values") if ov_active else None)
    except Exception:
        ov_active, ov_vals = False, None

    # recent decisions (safe even if LAST_DECISIONS missing)
    try:
        recent = list(LAST_DECISIONS)
    except Exception:
        recent = []

    return jsonify({
        "ok": True,
        "service": os.getenv("SERVICE_NAME", "bus"),
        "env": os.getenv("ENV", "prod"),
        "queue": q,

        "telemetry": {
            "agent": tel.get("agent_id"),
            "age": age_str,
            "age_sec": age_sec,
            "venues": venues_ct,
            "flat_tokens": flat_tokens_ct,
        },

        "policy": {
            "enabled": ENABLE_POLICY,
            "enforce": POLICY_ENFORCE,
            "path": POLICY_PATH,
            "loaded": _policy.loaded,
            "error": _policy.load_error,
            "overrides_active": ov_active,
            "overrides": ov_vals,
        },

        "last_decisions": recent,
    }), 200

@BUS.route("/policy/reload", methods=["POST"])
def policy_reload():
    _policy.maybe_load(force=True)
    return jsonify(ok=True, enabled=ENABLE_POLICY, enforce=POLICY_ENFORCE, path=POLICY_PATH,
                   loaded=_policy.loaded, error=_policy.load_error), 200

@BUS.route("/policy/evaluate", methods=["POST"])
def policy_evaluate():
    body, err = _require_json()
    if err: return err
    decision = _policy.evaluate_intent(body or {}, context={"telemetry": _last_tel})
    return jsonify(ok=True, decision=decision), 200

flask_app.register_blueprint(BUS)

# --- Receipts API (Edge → Cloud) ---------------------------------------------
from flask import Blueprint, request, jsonify
import os, hmac, hashlib
from logging import getLogger
log = getLogger("bus")

_receipts_bp = Blueprint("receipts", __name__)
_SEEN_IDS = set()  # in-proc idempotency; move to Postgres later

def _verify_hmac(sig: str, body: bytes) -> bool:
    EDGE_SECRET = os.getenv("EDGE_SECRET", "")  # must match Edge
    if not EDGE_SECRET:
        return False
    mac = hmac.new(EDGE_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, sig or "")

def _append_trade_row(norm: dict):
    # Uses your utils.get_gspread_client + SHEET_URL env
    from utils import get_gspread_client
    SHEET_URL = os.getenv("SHEET_URL", "")
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL missing")
    gc = get_gspread_client()
    sh = gc.open_by_url(SHEET_URL)
    ws = sh.worksheet("Trade_Log")   # make sure this tab exists

    row = [
        norm.get("timestamp_utc",""),
        norm.get("venue",""),
        norm.get("symbol",""),
        norm.get("side",""),
        norm.get("executed_qty",""),
        norm.get("avg_price",""),
        norm.get("quote_spent",""),
        norm.get("fee",""),
        norm.get("fee_asset",""),
        norm.get("order_id",""),
        "",  # client_order_id (optional)
        norm.get("txid",""),
        norm.get("status",""),
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")

@_receipts_bp.post("/api/receipts/ack")
def receipts_ack():
    raw = request.get_data()
    sig = request.headers.get("X-Nova-Signature","")
    if not _verify_hmac(sig, raw):
        return jsonify({"ok": False, "error": "bad signature"}), 401

    j = request.get_json(force=True)
    norm = (j.get("normalized") or {})
    rid  = norm.get("receipt_id") or f"{j.get('agent_id')}:{j.get('cmd_id')}"

    # idempotency in process
    if rid in _SEEN_IDS:
        return jsonify({"ok": True, "dedup": True})

    try:
        _append_trade_row(norm)
        _SEEN_IDS.add(rid)
        # (optional) Council Ledger
        try:
            from council_ledger import log_reckoning
            log_reckoning("receipt", True, "appended",
                          norm.get("symbol",""), norm.get("side",""),
                          norm.get("quote_spent",""), norm.get("venue",""),
                          "", "", norm.get("receipt_id",""))
        except Exception:
            pass
        return jsonify({"ok": True, "appended": True})
    except Exception as e:
        log.error(f"Trade log append failed: {e}")
        try:
            from council_ledger import log_reckoning
            log_reckoning("receipt", False, f"sheet append failed: {e}")
        except Exception:
            pass
        return jsonify({"ok": False, "error": f"sheet append failed: {e}"}), 500

# Register the blueprint on the production Flask app
flask_app.register_blueprint(_receipts_bp)

# --- Start Nova loops when the web app loads (once) -------------------------
try:
    from main import boot as _nova_boot
    _ = _nova_boot()  # returns True on success
except Exception as e:
    log.warning("Nova boot degraded: %s", e)

# ========== Errors ==========
@flask_app.errorhandler(404)
def _nf(_e): return jsonify(error="not_found"), 404
@flask_app.errorhandler(405)
def _me(_e): return jsonify(error="method_not_allowed"), 405
@flask_app.errorhandler(500)
def _ise(e): log.warning("Unhandled: %s", e); return jsonify(error="internal_error"), 500

# ========== Watchdogs / Daily ==========
def _policy_watchdog():
    while True:
        try: _policy.maybe_load()
        except Exception as e: log.debug("policy watchdog err: %s", e)
        time.sleep(10)

if ENABLE_POLICY:
    threading.Thread(target=_policy_watchdog, name="policy-watchdog", daemon=True).start()
    log.info("Policy watchdog started.")

DAILY_ENABLED   = _env_true("DAILY_ENABLED") or ENABLE_TELEGRAM
DAILY_UTC_HOUR  = int(os.getenv("DAILY_UTC_HOUR","9"))
DAILY_UTC_MIN   = int(os.getenv("DAILY_UTC_MIN","0"))
def _compose_daily() -> str:
    try: q = _queue_depth()
    except Exception: q = {}
    age = "-" if not _last_tel.get("ts") else f"{int(time.time())-int(_last_tel['ts'])}s"
    return ("☀️ <b>NovaTrade Daily</b>\n"
            f"as of {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
            f"<b>Telemetry age</b> {age}\n"
            f"<b>Queue</b> q:{q.get('queued',0)} l:{q.get('leased',0)} a:{q.get('acked',0)} f:{q.get('failed',0)}")
def _sleep_until(h:int,m:int):
    while True:
        now = datetime.now(timezone.utc)
        tgt = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if tgt <= now: tgt = tgt + timedelta(days=1)
        time.sleep((tgt-now).total_seconds()); yield
def _start_daily():
    if not (DAILY_ENABLED and ENABLE_TELEGRAM and _TELEGRAM_TOKEN and _TELEGRAM_CHAT): return
    def _loop():
        for _ in _sleep_until(DAILY_UTC_HOUR, DAILY_UTC_MIN):
            try: send_telegram(_compose_daily())
            except Exception as e: log.debug("daily send degraded: %s", e)
    threading.Thread(target=_loop, name="daily-report", daemon=True).start()
    log.info("Daily report scheduled for %02d:%02d UTC", DAILY_UTC_HOUR, DAILY_UTC_MIN)
_start_daily()

# --- DEBUG & TELEGRAM DIAGNOSTICS (restored) ---------------------------------
def _guess_base_url() -> Optional[str]:
    base = os.getenv("TELEGRAM_WEBHOOK_BASE") or os.getenv("OPS_BASE_URL")
    if base: return base.rstrip("/")
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    return f"https://{host}".rstrip("/") if host else None

def _tg_api(path: str) -> str:
    tok = _bot_token()
    if not tok:
        raise RuntimeError("BOT_TOKEN/TELEGRAM_BOT_TOKEN missing")
    return f"https://api.telegram.org/bot{tok}/{path}"

def _compute_webhook_url() -> Optional[str]:
    secret = os.getenv("TELEGRAM_WEBHOOK_SECRET")
    base = _guess_base_url()
    if not (secret and base):
        return None
    return f"{base}/tg/{secret}"

def _set_webhook_now() -> dict:
    import requests
    url = _compute_webhook_url()
    if not url:
        return {"ok": False, "reason": "missing TELEGRAM_WEBHOOK_SECRET or base URL"}
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

# ========== ASGI ==========
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; using WSGI: %s", e)
    app = flask_app  # type: ignore
