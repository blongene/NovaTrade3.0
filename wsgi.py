# wsgi.py — NovaTrade Bus (Phase 7A: policy wired with telemetry context)
# FULL INTEGRITY VERSION: Preserves all logic, fixes HMAC, fixes NameError.
from __future__ import annotations
import os, json, hmac, hashlib, logging, threading, time, uuid, re
from functools import wraps
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple
from flask import Flask, request, jsonify, Blueprint
from bus_store_pg import get_store, OUTBOX_LEASE_SECONDS
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sheets_bp import SHEETS_ROUTES, start_background_flusher
from telemetry_routes import bp_telemetry
from autonomy_modes import get_autonomy_state
from ops_api import bp as ops_bp

# Phase 29 safety: one-line boot config health (warnings only)
try:
    from bus_config_doctor import emit_once as _bus_config_emit_once  # type: ignore
except Exception:
    _bus_config_emit_once = None  # type: ignore

# Telegram surface (buttons/webhook). Safe if telegram is disabled/misconfigured.
try:
    from telegram_webhook import tg_blueprint as bp_telegram, set_telegram_webhook  # type: ignore
except Exception:
    bp_telegram = None  # type: ignore
    def set_telegram_webhook() -> None:  # type: ignore
        return

# Phase 24C+ / 28.2 Authority Gate
# Prefer the newer authority_gate module when present, but fall back to
# legacy edge_authority to keep older deployments working.
try:
    from authority_gate import evaluate_agent, lease_block_response  # type: ignore
except Exception:
    from edge_authority import evaluate_agent, lease_block_response

# ========== Logging ==========
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bus")
logging.getLogger("werkzeug").setLevel(logging.WARNING if LOG_LEVEL != "DEBUG" else logging.DEBUG)

# Emit one concise config line at boot (no behavior change)
try:
    if _bus_config_emit_once:
        _bus_config_emit_once(prefix="BUS_CONFIG")
except Exception:
    pass

try:
    if _bus_config_emit_once:
        _bus_config_emit_once(prefix="BUS_CONFIG")
except Exception:
    pass

# ========== Flask ==========
flask_app = Flask(__name__)
store = get_store()

def _register_bp_once(app: Flask, bp: Any, url_prefix: str | None = None) -> None:
    """Register a blueprint exactly once.

    Render/Uvicorn will import wsgi.py; accidental duplicate registration
    should not crash the service.
    """
    if bp is None:
        return
    try:
        name = getattr(bp, "name", None) or ""
        if name and name in getattr(app, "blueprints", {}):
            return
        app.register_blueprint(bp, url_prefix=url_prefix)
    except ValueError:
        # Blueprint already registered
        return

_register_bp_once(flask_app, SHEETS_ROUTES, url_prefix="/sheets")

# Mount Telegram blueprint at /tg (health: /tg/health; webhook: /tg/webhook; prompt: /tg/prompt)
_register_bp_once(flask_app, bp_telegram, url_prefix="/tg")
try:
    set_telegram_webhook()
except Exception as e:
    log.warning("telegram webhook set failed: %r", e)

# ---- Outbox shims (route-safe; delegate to Postgres store) ----
def _enqueue_command(cmd_id: str, payload: dict) -> None:
    p = dict(payload or {})
    p.setdefault("id", cmd_id)
    agent = p.get("agent_id") or "cloud"
    # idempotent enqueue by payload hash (handled in store)
    store.enqueue(agent, p)

def _pull_commands(agent_id: str, max_items: int = 10, lease_seconds: int = 90) -> list[dict]:
    leased = store.lease(agent_id, max_items)
    # normalize to the legacy shape used by your handlers
    out = []
    for row in leased:
        out.append({"id": str(row.get("id")), "payload": _canonicalize_order_place_intent(row.get("intent") or {})})
    return out

def _ack_command(cmd_id: str, agent_id: str, status: str, detail: dict | None = None) -> None:
    ok = (str(status).lower() == "ok")
    store.save_receipt(agent_id, int(cmd_id) if str(cmd_id).isdigit() else None, detail or {}, ok)
    if ok and str(cmd_id).isdigit():
        store.done(int(cmd_id))

def _queue_depth() -> dict:
    s = store.stats()
    # map to keys your dash/health expect
    return {
        "queued": int(s.get("queued", 0)),
        "leased": int(s.get("leased", 0)),
        "acked":  int(s.get("done",   0)),
        "failed": 0,
    }
  
# Track which commands we've already logged to Trade_Log in this process.
TRADE_LOGGED_CMDS = set()
TRADE_LOG_STUB_WARNED = set()


# ---------- Command context cache (for richer Trade_Log receipts) ----------
# Some store backends (e.g., PGStore) do not expose store.get(cmd_id).
# To avoid stub Trade_Log rows, we cache recent command dicts by id when:
# - /ops/enqueue succeeds
# - /api/commands/pull leases commands
#
# This cache is best-effort, bounded, and safe: it never blocks ACK.
CMD_CTX_CACHE = {}
CMD_CTX_CACHE_ORDER = []
CMD_CTX_CACHE_MAX = int(os.getenv("CMD_CTX_CACHE_MAX", "500"))

def _cache_cmd_ctx(cmd_id, cmd_obj):
    try:
        if cmd_id is None:
            return
        key = str(cmd_id)
        if not isinstance(cmd_obj, dict):
            return
        # Keep only light context
        slim = {
            "id": cmd_obj.get("id", cmd_id),
            "agent_id": cmd_obj.get("agent_id") or cmd_obj.get("agent") or cmd_obj.get("leased_by"),
            "intent": cmd_obj.get("intent") or cmd_obj.get("payload") or cmd_obj.get("command") or {},
            "payload": cmd_obj.get("payload") or cmd_obj.get("intent") or cmd_obj.get("command") or {},
            "kind": cmd_obj.get("kind") or cmd_obj.get("type") or (cmd_obj.get("intent") or {}).get("type"),
            "hash": cmd_obj.get("hash") or cmd_obj.get("intent_hash"),
        }
        CMD_CTX_CACHE[key] = slim
        CMD_CTX_CACHE_ORDER.append(key)
        # bound
        while len(CMD_CTX_CACHE_ORDER) > CMD_CTX_CACHE_MAX:
            old = CMD_CTX_CACHE_ORDER.pop(0)
            CMD_CTX_CACHE.pop(old, None)
    except Exception:
        # never raise from cache path
        return

def _get_cached_cmd_ctx(cmd_id):
    try:
        if cmd_id is None:
            return None
        return CMD_CTX_CACHE.get(str(cmd_id))
    except Exception:
        return None

# ========== Flags / helpers ==========
def _env_true(k: str) -> bool:
    return os.environ.get(k, "").lower() in ("1","true","yes","on")

def _canonical(d: dict) -> bytes:
    return json.dumps(d, separators=(",",":"), sort_keys=True).encode("utf-8")


# ========== Intent canonicalization (Edge compatibility) ==========
# Some command producers send order.place intents as:
#   {"type":"order.place", "payload": { ... amount_usd ... }}
# while some Edge executors expect sizing fields (amount_usd/amount_quote/amount_base)
# at the top-level. This helper promotes common fields from payload -> root and
# guarantees type/side normalization without breaking backwards compatibility.

def _canonicalize_order_place_intent(intent: dict) -> dict:
    try:
        if not isinstance(intent, dict):
            return {}
        # Shallow copy so we never mutate DB objects in-place
        out = dict(intent)
        payload = out.get("payload")
        if not isinstance(payload, dict):
            payload = {}

        # If this isn't an order.place, do nothing
        itype = (out.get("type") or payload.get("type") or "").strip()
        if itype and itype != "order.place":
            return out
        # Heuristic: treat as order.place when side present in payload/root
        if not itype and not (out.get("side") or payload.get("side")):
            return out

        out["type"] = "order.place"

        # Promote common fields
        promote_keys = [
            "venue","symbol","token","side","mode","note","flags",
            "amount_usd","amount_quote","amount_base",
            "price","price_usd","limit_price","time_in_force",
            "dry_run","idempotency_key","client_order_id","meta"
        ]
        for k in promote_keys:
            if out.get(k) in (None, "", [], {}):
                v = payload.get(k)
                if v not in (None, ""):
                    out[k] = v

        # Normalize side
        if isinstance(out.get("side"), str):
            out["side"] = out["side"].upper()

        # Quote sizing default: many producers use amount_usd; Edge may read amount_quote
        if out.get("amount_quote") in (None, "", 0, 0.0) and out.get("amount_usd") not in (None, "", 0, 0.0):
            out["amount_quote"] = out.get("amount_usd")

        # Float coercion (safe)
        for nk in ("amount_usd","amount_quote","amount_base","price","price_usd","limit_price"):
            if nk in out and out[nk] not in (None, ""):
                try:
                    out[nk] = float(out[nk])
                except Exception:
                    pass

        # Backfill payload with promoted fields so older consumers still find them there
        if payload is not None:
            new_payload = dict(payload)
            for k in promote_keys:
                if new_payload.get(k) in (None, "", [], {}):
                    if out.get(k) not in (None, ""):
                        new_payload[k] = out.get(k)
            out["payload"] = new_payload

        return out
    except Exception:
        # Never fail routing because of canonicalization
        return intent if isinstance(intent, dict) else {}


def _canonicalize_leased_commands(rows: list) -> list:
    """Normalize leased command rows into an Edge-safe shape."""
    out = []
    for row in (rows or []):
        if not isinstance(row, dict):
            out.append(row)
            continue
        r = dict(row)
        # common shapes: {id, intent:{...}} or {id, payload:{...}}
        if isinstance(r.get("intent"), dict):
            it = r["intent"]
            if (it.get("type") == "order.place") or (isinstance(it.get("payload"), dict) and (it.get("payload") or {}).get("side")):
                r["intent"] = _canonicalize_order_place_intent(it)
        if isinstance(r.get("payload"), dict):
            it = r["payload"]
            if (it.get("type") == "order.place") or (isinstance(it.get("payload"), dict) and (it.get("payload") or {}).get("side")):
                r["payload"] = _canonicalize_order_place_intent(it)
        out.append(r)
    return out
# ========== Telegram ==========
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

# ========== HMAC (ROBUST PATCH) ==========
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET", "")
TELEMETRY_SECRET = os.getenv("TELEMETRY_SECRET", OUTBOX_SECRET)
EDGE_SECRET = os.getenv("EDGE_SECRET", "")
REQUIRE_HMAC_OPS = _env_true("REQUIRE_HMAC_OPS")
REQUIRE_HMAC_PULL = _env_true("REQUIRE_HMAC_PULL")
REQUIRE_HMAC_TELEMETRY = _env_true("REQUIRE_HMAC_TELEMETRY")

def _verify_hmac_json(secret_env: str, header_name):
    """Verify an HMAC-SHA256 signature over the request body.

    Compatibility rules (to tolerate older Edge builds):
    - Accepts a single header name or a list/tuple of names.
    - Tries verifying against:
        1) the raw request bytes
        2) canonical JSON bytes (sorted keys, compact separators)

    Returns: (ok, body_dict, provided_sig, expected_sig)
    """

    raw = request.get_data() or b""
    try:
        body = json.loads(raw.decode("utf-8") or "{}")
    except Exception:
        body = {}

    secret = os.getenv(secret_env, "")

    # Accept a single header string or multiple possible header names
    if isinstance(header_name, (list, tuple)):
        header_names = list(header_name)
    else:
        header_names = [header_name]

    # Always tolerate common legacy names
    for hn in ("X-OUTBOX-SIGN", "X-Signature", "X-SIGNATURE"):
        if hn not in header_names:
            header_names.append(hn)

    provided = ""
    for hn in header_names:
        v = request.headers.get(hn, "") or ""
        if v:
            provided = v
            break

    if not secret or not provided:
        # signal that we couldn't even attempt verification
        return False, body, provided or "", "missing_secret_or_sig"

    # 1) Verify against raw bytes
    expected_raw = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    if hmac.compare_digest(expected_raw, provided):
        return True, body, provided, expected_raw

    # 2) Verify against canonical JSON
    try:
        canon = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        expected_canon = hmac.new(secret.encode("utf-8"), canon, hashlib.sha256).hexdigest()
        if hmac.compare_digest(expected_canon, provided):
            return True, body, provided, expected_canon
    except Exception:
        pass

    return False, body, provided, expected_raw

def _require_json():
    if not request.is_json: return None, (jsonify(ok=False, error="invalid_or_missing_json"), 400)
    try: return request.get_json(force=True, silent=False), None
    except Exception: return None, (jsonify(ok=False, error="malformed_json"), 400)

# ========== Kill switches & Policy flags ==========
CLOUD_HOLD     = _env_true("CLOUD_HOLD")
NOVA_KILL      = _env_true("NOVA_KILL")
try:
    from kill_switches import cloud_hold_active as _cloud_hold_active, cloud_hold_reason as _cloud_hold_reason
except Exception:  # ultra-safe fallback
    def _cloud_hold_active():
        return bool(NOVA_KILL or CLOUD_HOLD)
    def _cloud_hold_reason():
        return "NOVA_KILL" if NOVA_KILL else ("CLOUD_HOLD" if CLOUD_HOLD else "")
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
    if REQUIRE_HMAC_OPS:
        ok, _, _, _ = _verify_hmac_json("OUTBOX_SECRET", "X-NT-Sig")
        if not ok:
            return jsonify(ok=False, error="invalid_signature"), 401
            
    values = body if isinstance(body, dict) else {}
    ttl = int(values.pop("ttl_sec", 3600) or 3600)
    _policy_overrides["values"] = values
    _policy_overrides["ttl_expiry"] = time.time() + ttl
    return jsonify(ok=True, applied=values, ttl_sec=ttl), 200

# ========== Health/root ==========
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info = {"ok": True, "web": "up", "db": "postgres"}
    info["policy"] = {"enabled": ENABLE_POLICY, "enforce": POLICY_ENFORCE,
                      "path": POLICY_PATH, "loaded": _policy.loaded, "error": _policy.load_error}
    try: info["queue"] = _queue_depth()
    except Exception as e: info["queue_error"] = str(e)
    return jsonify(info), 200

@flask_app.get("/health")
def health():
    """Compatibility alias for older clients/scripts."""
    return healthz()

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
    """Edge → Bus: telemetry (balances + meta)."""
    ok, body, provided, expected = _verify_hmac_json("TELEMETRY_SECRET", "X-TELEMETRY-SIGN")
    if REQUIRE_HMAC_TELEMETRY and not ok:
        return jsonify(ok=False, error="invalid_signature"), 401

    agent_id = body.get("agent_id") or "edge"
    flat, by_venue = _normalize_balances(body.get("balances") or {})

    now_ts = int(time.time())
    global _last_tel
    _last_tel.update(
        {
            "agent_id": agent_id,
            "flat": flat,
            "by_venue": by_venue,
            "ts": now_ts,
        }
    )

    # NEW: also feed telemetry_routes cache + SQLite store
    try:
        from telemetry_routes import update_from_push

        update_from_push(
            agent=agent_id,
            balances=by_venue,
            aggregates={"last_push": {"agent": agent_id, "ts": now_ts}},
            ts=now_ts,
        )
    except Exception as e:
        log.info("telemetry_push: unable to update telemetry_routes cache: %s", e)

    venues_line = ", ".join(f"{v}:{len(t)}" for v, t in by_venue.items()) or "—"
    log.info(
        "📡 Telemetry from %s | venues=%s | flat_tokens=%d",
        agent_id,
        venues_line,
        len(flat),
    )
    return jsonify(ok=True, received=(len(by_venue) or len(flat))), 200

@flask_app.post("/api/telemetry/push_balances")
def telemetry_push_balances():
    """Edge → Bus: periodic balance snapshots (HMAC with TELEMETRY_SECRET)."""
    # Use robust verify
    ok, body, provided, expected = _verify_hmac_json("TELEMETRY_SECRET", "X-TELEMETRY-SIGN")
    if REQUIRE_HMAC_TELEMETRY and not ok:
        return jsonify(ok=False, error="invalid_signature"), 401

    # --- normalize multiple payload shapes ---
    root = dict(body)  # shallow copy
    bal = root.get("balances") or {}

    agent_id = root.get("agent") or root.get("agent_id") or bal.get("agent") or "edge"

    by_venue = (
        root.get("by_venue") or
        bal.get("by_venue")  or
        {}
    )
    flat = (
        root.get("flat") or
        bal.get("flat")  or
        {}
    )

    ts = (
        root.get("ts") or root.get("timestamp") or root.get("time") or
        bal.get("ts")  or bal.get("timestamp")  or bal.get("time")
    )

    # Ensure dicts
    if not isinstance(by_venue, dict): by_venue = {}
    if not isinstance(flat, dict):     flat = {}

    venues_line = ",".join(by_venue.keys())
    flat_count  = len(flat)
    venue_count = len(by_venue)

    log.info(
        "📊 Telemetry snapshot from %s — venues=[%s] tokens=%d ts=%s",
        agent_id,
        venues_line,
        flat_count,
        ts,
    )

    # Update global last snapshot for mirror jobs
    now_ts = int(time.time())
    global _last_tel
    _last_tel = {
        "agent_id": agent_id,
        "by_venue": by_venue,
        "flat": flat,
        "ts": now_ts,
    }

    # NEW: also update telemetry_routes cache + SQLite
    try:
        from telemetry_routes import update_from_push

        update_from_push(
            agent=agent_id,
            balances=by_venue,
            aggregates={"last_push": {"agent": agent_id, "ts": now_ts}},
            ts=now_ts,
        )
    except Exception as e:
        log.info("telemetry_push_balances: unable to update telemetry_routes cache: %s", e)

    return jsonify(ok=True, received=flat_count), 200

@flask_app.get("/api/telemetry/last")
def telemetry_last():
    """
    Simple JSON view of the last telemetry snapshot.

    Phase 19:
      • Prefer the new DB-backed telemetry_routes state
        (Edge → Bus via /api/telemetry/push).
      • Fallback to the legacy _last_tel dict for backward compatibility.

    This keeps telemetry_mirror.py and any external callers working
    without caring how telemetry is ingested.
    """
    # --- Preferred path: telemetry_routes (DB + in-memory caches) ---
    try:
        from telemetry_routes import (
            get_latest_balances,
            get_latest_aggregates,
            get_telemetry_age_sec,
        )

        balances = get_latest_balances() or {}
        aggregates = get_latest_aggregates() or {}
        age_sec = float(get_telemetry_age_sec() or 0.0)

        if balances:
            # Build a flat token view by summing across venues
            flat: Dict[str, float] = {}
            for venue, assets in balances.items():
                if not isinstance(assets, dict):
                    continue
                for asset, qty in (assets or {}).items():
                    try:
                        qf = float(qty or 0.0)
                    except Exception:
                        continue
                    sym = str(asset).upper()
                    flat[sym] = flat.get(sym, 0.0) + qf

            # Try to recover an agent id from aggregates
            agent = "edge"
            hb = aggregates.get("last_heartbeat") or {}
            if isinstance(hb, dict):
                agent = hb.get("agent") or agent

            # ts is approximate: "now minus age"
            import time as _time
            if age_sec and age_sec < 9e8:
                ts = int(_time.time() - age_sec)
            else:
                ts = int(_time.time())

            data = {
                "agent": agent,
                "flat": flat,
                "by_venue": balances,
                "ts": ts,
            }
            return jsonify(ok=True, data=data, source="telemetry_routes"), 200

    except Exception as e:
        log.warning("telemetry_last: telemetry_routes path degraded: %s", e)

    # --- Legacy fallback: use _last_tel if telemetry_routes has nothing ---
    global _last_tel
    data = dict(_last_tel or {})
    return jsonify(ok=True, data=data, source="legacy"), 200

@flask_app.get("/api/telemetry/health")
def telemetry_health():
    """
    Lightweight health summary for Edge telemetry.

    It reuses the same data that /api/telemetry/last serves:
      • Prefer telemetry_routes' in-memory state if it has balances.
      • Otherwise fall back to the legacy _last_tel dict that
        /api/telemetry/push_balances keeps updated.

    No HTTP self-calls, no DB queries here – it’s meant to be cheap and safe.
    """
    import time as _time

    max_age = float(os.getenv("TELEMETRY_HEALTH_MAX_AGE_SEC", "900"))  # 15m default

    # --- Preferred path: telemetry_routes cache, if it actually has data ---
    try:
        from telemetry_routes import (
            get_latest_balances,
            get_latest_aggregates,
            get_telemetry_age_sec,
        )

        balances = get_latest_balances() or {}
        aggregates = get_latest_aggregates() or {}
        age_sec = float(get_telemetry_age_sec() or 0.0)

        if balances:
            if not isinstance(balances, dict):
                balances = {}
            if not isinstance(aggregates, dict):
                aggregates = {}

            venues = sorted(balances.keys())
            aggregates_keys = sorted(aggregates.keys())
            agent = "edge"
            hb = aggregates.get("last_heartbeat") or {}
            if isinstance(hb, dict):
                agent = hb.get("agent") or agent

            ok_flag = bool(venues) and (age_sec < max_age if age_sec else True)

            return jsonify(
                {
                    "ok": ok_flag,
                    "age_sec": age_sec,
                    "venues": venues,
                    "aggregates_keys": aggregates_keys,
                    "agent": agent,
                    "source": "telemetry_routes",
                }
            ), 200

    except Exception as e:
        log.warning("telemetry_health: telemetry_routes path degraded: %s", e)

    # --- Fallback path: use the legacy _last_tel snapshot ---
    global _last_tel
    data = dict(_last_tel or {})

    by_venue = data.get("by_venue") or {}
    if not isinstance(by_venue, dict):
        by_venue = {}

    venues = sorted(by_venue.keys())

    ts = data.get("ts") or 0
    if ts:
        try:
            age_sec = max(0.0, _time.time() - float(ts))
        except Exception:
            age_sec = 9e9
    else:
        age_sec = 9e9

    agent = data.get("agent") or data.get("agent_id") or ""

    ok_flag = bool(venues) and (age_sec < max_age if age_sec else False)

    return jsonify(
        {
            "ok": ok_flag,
            "age_sec": age_sec,
            "venues": venues,
            "aggregates_keys": [],
            "agent": agent,
            "source": "legacy",
        }
    ), 200

@flask_app.post("/api/edge/balances")
def edge_balances():
    """Edge-authenticated balance push (HMAC: EDGE_SECRET)."""
    # Use robust verify
    ok, body, provided, expected = _verify_hmac_json("EDGE_SECRET", "X-Nova-Signature")
    if not ok:
        return jsonify(ok=False, error="invalid_signature"), 401

    root = dict(body)
    bal  = root.get("balances") or {}

    agent_id = root.get("agent") or root.get("agent_id") or bal.get("agent") or "edge"
    by_venue = root.get("by_venue") or bal.get("by_venue") or {}
    flat     = root.get("flat")     or bal.get("flat")     or {}
    ts       = (root.get("ts") or root.get("timestamp") or root.get("time") or
                bal.get("ts")  or bal.get("timestamp")  or bal.get("time"))

    if not isinstance(by_venue, dict): by_venue = {}
    if not isinstance(flat, dict):     flat = {}

    venues_line = ",".join(by_venue.keys())
    log.info("🤝 EDGE balances from %s — venues=[%s] tokens=%d ts=%s",
             agent_id, venues_line, len(flat), ts)

    return jsonify(ok=True, received=len(flat), venues=len(by_venue)), 200

@flask_app.post("/bus/push_balances")
def telemetry_push_aliases():
    return telemetry_push()

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
    # Robust verify
    ok, body, _, _ = _verify_hmac_json("OUTBOX_SECRET", "X-NT-Sig")
    if REQUIRE_HMAC_OPS and not ok:
        return jsonify(ok=False, error="invalid_signature"), 401

    if _cloud_hold_active():
        return jsonify(ok=False, error="bus_killed", reason=_cloud_hold_reason()), 503

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
        "decision_id": body.get("decision_id") or body.get("decisionId") or "",
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
        policy_cfg = dict(getattr(_policy.engine, "cfg", {}) or {})
        route_res = router.choose_venue(intent, _last_tel or {}, policy_cfg)
        if route_res.get("ok"):
            intent.update(route_res.get("patched_intent") or {})
            intent.setdefault("flags", []).extend(route_res.get("flags") or [])
            try:
                _last_intent_at[(intent.get("venue"), intent["symbol"], intent["side"])] = time.time()
            except Exception:
                pass
        else:
            LAST_DECISIONS.append({"intent": intent, "decision": route_res, "ts": int(time.time())})
            return jsonify(ok=False, policy="blocked",
                           reason=route_res.get("reason", "routing_failed"),
                           decision=route_res), 403
    except Exception as e:
        log.info("router degraded: %s", e)

    # ✅ === Phase 10 Predictive Policy Bias ===
    try:
        from predictive_policy_driver import apply_predictive_bias
        patch = apply_predictive_bias(intent)
        if patch and patch.get("patched_intent"):
            intent.update(patch.get("patched_intent", {}))
            intent.setdefault("flags", []).extend(patch.get("flags", []))
            log.info(f"Applied predictive bias {patch.get('factor'):.3f} conf={patch.get('confidence'):.2f}")
    except Exception as e:
        log.info(f"predictive bias degraded: {e}")

    # --- policy evaluation with telemetry context -----------------------------
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

@BUS.route("/receipts/last", methods=["GET"])
def receipts_last():
    return jsonify(ok=True, receipts=[]), 200

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

# --- Uniform Edge HMAC for pull/ack ------------------------------------------
import os, hmac, hashlib
from functools import wraps
from flask import request, jsonify

# Enqueue (cloud-side) — assumes your existing HMAC verify wrapper outside
@flask_app.before_request
def ping_prevent_cold_start():
    request.start_time = time.time()

@flask_app.post("/ops/enqueue")
def ops_enqueue():
    """Enqueue a command into the outbox.

    Accepts multiple payload shapes for backwards compatibility:
      - {"payload": {"agent_id": "...", "command": {...}}}
      - {"agent_id": "...", "command": {...}}
      - Aliases: agent, agentId, agent_name, agentName, target_agent
    """
    j = request.get_json(force=True) or {}

    payload = j.get("payload")
    if not isinstance(payload, dict):
        payload = {}

    # Resolve agent_id from common aliases (payload-first, then top-level)
    agent_id = (
        payload.get("agent_id")
        or payload.get("agent")
        or payload.get("agentId")
        or payload.get("agent_name")
        or payload.get("agentName")
        or payload.get("target_agent")
        or payload.get("agent_target")
        or j.get("agent_id")
        or j.get("agent")
        or j.get("agentId")
        or j.get("agent_name")
        or j.get("agentName")
        or j.get("target_agent")
        or j.get("agent_target")
        or "cloud"
    )

    # Resolve intent/command (payload-first, then top-level)
    intent = (
        payload.get("command")
        or payload.get("intent")
        or payload.get("payload")
        or j.get("command")
        or j.get("intent")
        or j.get("payload")
        or {}
    )
    if not isinstance(intent, dict):
        intent = {"raw": intent}

    # Optional idempotency / dedupe key (kept OUTSIDE the intent payload by design)
    # We accept multiple common aliases.
    idempotency_key = (
        payload.get("idempotency_key")
        or payload.get("idempotencyKey")
        or payload.get("dedupe_key")
        or payload.get("dedupeKey")
        or j.get("idempotency_key")
        or j.get("idempotencyKey")
        or j.get("dedupe_key")
        or j.get("dedupeKey")
    )
    if isinstance(idempotency_key, (int, float)):
        idempotency_key = str(idempotency_key)
    if not isinstance(idempotency_key, str):
        idempotency_key = None

    try:
        # Prefer explicit idempotency_key for dedupe when supplied.
        res = store.enqueue(agent_id, intent, idempotency_key=idempotency_key)
        # Cache minimal context for Trade_Log correlation
        _cache_cmd_ctx(res.get("id"), {"id": res.get("id"), "agent_id": agent_id, "intent": intent, "payload": intent, "hash": res.get("hash")})
        # Expect res like: {"ok": True, "id": ..., "status": "queued", "hash": "..."}
        log.info(
            "ops_enqueue: agent=%s ok=%s id=%s status=%s hash=%s",
            agent_id,
            res.get("ok"),
            res.get("id"),
            res.get("status"),
            res.get("hash"),
        )
        return jsonify(res)
    except Exception as e:
        log.exception("ops_enqueue error")
        return jsonify({"ok": False, "error": str(e)}), 500
    except Exception as e:
        log.exception("ops_enqueue failed for agent=%s: %s", agent_id, e)
        return jsonify(ok=False, error=str(e)), 500

@flask_app.after_request
def add_server_timing_header(response):
    delta = (time.time() - getattr(request, "start_time", time.time())) * 1000
    response.headers["Server-Timing"] = f"app;dur={delta:.2f}"
    return response
           

# Edge pulls leased commands
@flask_app.post("/api/commands/pull")
def cmd_pull():
    # Robust verify (expects HMAC under X-OUTBOX-SIGN using OUTBOX_SECRET)
    ok, body, provided, expected = _verify_hmac_json("OUTBOX_SECRET", "X-OUTBOX-SIGN")
    if not ok:
        log.error("cmd_pull: invalid HMAC provided=%s expected=%s", provided, expected)
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "invalid_signature",
                    "provided": provided,
                    "expected": expected,
                }
            ),
            401,
        )

    agent = (body.get("agent_id") or body.get("agent") or body.get("agent_target") or "edge").strip()
    try:
        n = int(body.get("limit") or body.get("max_items") or body.get("n") or 5)
    except Exception:
        n = 5
    n = max(1, min(n, 25))

    # Phase 24C+ trust boundary
    trusted, reason, age = evaluate_agent(agent)

    # If cloud hold is active, stop dispatch (keep 200 to avoid retry storms)
    if _cloud_hold_active():
        return jsonify(
            {
                "ok": True,
                "commands": [],
                "lease_seconds": OUTBOX_LEASE_SECONDS,
                "hold": True,
                "reason": _cloud_hold_reason(),
                "agent_id": agent,
                "age_sec": age,
            }
        )

    # If edge authority is enabled and agent is not trusted, do not dispatch.
    if not trusted:
        resp = lease_block_response(agent)
        resp["lease_seconds"] = OUTBOX_LEASE_SECONDS
        return jsonify(resp)

    # Lease commands for this agent
    try:
        out = store.lease(agent, n) or []
        # Canonicalize intents before sending to Edge (backward compatible)
        out = _canonicalize_leased_commands(out)
    except Exception as e:
        log.exception("cmd_pull: lease error agent=%s", agent)
        return jsonify({"ok": False, "error": f"lease_error: {e}"}), 500

    return jsonify(
        {
            "ok": True,
            "commands": out,
            "lease_seconds": OUTBOX_LEASE_SECONDS,
            "hold": False,
            "reason": reason,
            "agent_id": agent,
            "age_sec": age,
        }
    )


def append_trade_log_safe(cmd_id, agent_id, receipt, status: str, ok_val: bool):
    """
    Best-effort logging of edge receipts into Trade_Log.

    - Idempotent per cmd_id within this process (TRADE_LOGGED_CMDS).
    - If the command is no longer in the store, uses a stub but only logs
      a single info-level line per cmd_id (TRADE_LOG_STUB_WARNED).
    - Any exception is caught so /api/commands/ack stays clean.
    """
    try:
        # Normalize cmd_id -> int or None
        try:
            cid = int(cmd_id) if cmd_id is not None else None
        except Exception:
            cid = None

        # Idempotent: don't log the same command twice in this process
        if cid is not None and cid in TRADE_LOGGED_CMDS:
            log.info("trade_log: cmd %s already logged; skipping duplicate", cid)
            return

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            # Sheets logging is optional; skip quietly if not configured
            log.debug("trade_log: SHEET_URL missing; skipping Trade_Log append")
            return

        # Build gspread client using the project's helper(s)
        try:
            from utils import get_gspread_client  # modern name
        except ImportError:
            from utils import build_gspread_client as get_gspread_client  # legacy

        gc_client = get_gspread_client()

        # Try to fetch the original command for richer context
        command = _get_cached_cmd_ctx(cid)
        if command is None and cid is not None:
            # Some store backends do not implement .get(); try it only if present.
            try:
                if hasattr(store, "get") and callable(getattr(store, "get")):
                    command = store.get(cid)
            except Exception:
                command = None
                if cid not in TRADE_LOG_STUB_WARNED:
                    TRADE_LOG_STUB_WARNED.add(cid)
                    log.info(
                        "trade_log: no command %s in store; using stub for sheet append",
                        cid,
                    )

        if not isinstance(command, dict):
            command = {"id": cid, "agent_id": agent_id, "intent": command or {}}

        # --- Normalize store.get() shapes into a consistent dict -----------
        # store.get() may return {"intent": {...}} or {"payload": {...}} depending on backend.
        # Ensure BOTH keys exist so downstream log_trade_to_sheet can reliably find decision_id.
        try:
            if isinstance(command, dict):
                if isinstance(command.get("intent"), dict) and not isinstance(command.get("payload"), dict):
                    command["payload"] = command["intent"]
                elif isinstance(command.get("payload"), dict) and not isinstance(command.get("intent"), dict):
                    command["intent"] = command["payload"]
        except Exception:
            pass

        # Normalize receipt for logging
        if not isinstance(receipt, dict):
            receipt = {"raw": receipt}

        # Ensure status/ok are present in the receipt we write
        receipt = {
            **receipt,
            "status": status,
            "ok": bool(ok_val),
        }

        # Existing helper that writes to the Trade_Log sheet
        log_trade_to_sheet(gc_client, sheet_url, command, receipt)

        if cid is not None:
            TRADE_LOGGED_CMDS.add(cid)

    except Exception:
        # Sheets/logging issues must NEVER break ACK
        log.exception("trade_log: append degraded (non-fatal)")

# Edge ACKs execution results
@flask_app.post("/api/commands/ack")
def cmd_ack():
    """
    Edge Agent sends a receipt for a previously queued command.

    Responsibilities:
      * Verify HMAC using OUTBOX_SECRET + X-OUTBOX-SIGN
      * Persist the receipt in the outbox store
      * Mark the command as done / failed so it stops being re-leased
      * Best-effort Trade_Log append (idempotent, non-fatal)
    """
    # ---- 1) HMAC verification ----------------------------------------------
    ok, body, provided, expected = _verify_hmac_json("OUTBOX_SECRET", "X-OUTBOX-SIGN")
    if not ok:
        cmd_id = (body or {}).get("id") or (body or {}).get("cmd_id")
        log.error(
            "cmd_ack: invalid HMAC for id=%s provided=%s expected=%s",
            cmd_id,
            provided,
            expected,
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "invalid_signature",
                    "provided": provided,
                    "expected": expected,
                }
            ),
            401,
        )

    # ---- 2) Normalize fields -----------------------------------------------
    receipt = body.get("receipt") or {}

    agent_id = (
        (body.get("meta") or {}).get("agent_id")
        or body.get("agent_id")
        or body.get("agent")
        or "edge"
    ).strip()

    cmd_id = body.get("id") or body.get("cmd_id")
    if cmd_id is None:
        return jsonify({"ok": False, "error": "missing cmd id"}), 400

    # status can come from wrapper body or receipt
    status = (body.get("status") or receipt.get("status") or "").strip().lower()

    # ok can come from wrapper body OR receipt; if absent, infer from status
    ok_raw = body.get("ok", None)
    if ok_raw is None:
        ok_raw = receipt.get("ok", None)

    if ok_raw is None:
        # infer if not explicitly provided
        ok_val = status not in ("error", "failed", "held")
    else:
        ok_val = bool(ok_raw)

    if not status:
        status = "ok" if ok_val else "error"

    # ---- 3) Persist receipt + mark command done/failed ----------------------
    try:
        cmd_id_int = int(cmd_id)
    except Exception:
        cmd_id_int = None

    try:
        # Always record the receipt in the outbox store
        store.save_receipt(agent_id, cmd_id_int, receipt, ok=ok_val)

        # Then mark command status so it stops being re-leased
        if cmd_id_int is not None:
            if ok_val:
                store.done(cmd_id_int)
            else:
                reason = (
                    receipt.get("error")
                    or receipt.get("message")
                    or status
                    or "error"
                )
                try:
                    store.fail(cmd_id_int, reason)
                except TypeError:
                    # back-compat in case fail(self, cmd_id) exists somewhere
                    store.fail(cmd_id_int)
    except Exception:
        log.exception("cmd_ack: failed to persist receipt / mark status for id=%s", cmd_id)

    # ---- 3.5) Operator-visible ack line (grep target) -----------------------
    try:
        buslog = logging.getLogger("bus")
        ok_str = "true" if ok_val else "false"
        buslog.info("ops_ack: agent=%s cmd=%s status=%s ok=%s", agent_id, cmd_id, status, ok_str)
    except Exception:
        log.exception("cmd_ack: failed to write ops_ack log for id=%s", cmd_id)

    # ---- 4) Best-effort, idempotent Trade_Log append ------------------------
    append_trade_log_safe(cmd_id, agent_id, receipt, status=status, ok_val=ok_val)

    # ---- 5) Final JSON response back to Edge --------------------------------
    return jsonify({"ok": True})


@flask_app.get("/api/debug/outbox")
def dbg_outbox():
    return jsonify(store.stats())

@flask_app.get("/api/debug/outbox_list")
def outbox_list():
    import psycopg2, os
    cx = psycopg2.connect(os.environ["DB_URL"]); cur = cx.cursor()
    cur.execute("select id, status, leased_by, lease_expires_at from commands order by id desc limit 100;")
    rows = [{"id":r[0], "status":r[1], "leased_by":r[2], "lease_expires_at":r[3].isoformat() if r[3] else None} for r in cur.fetchall()]
    cx.close()
    return jsonify({"rows": rows})

@flask_app.post("/api/debug/unlease_all")
def unlease_all():
    import psycopg2, os
    cx = psycopg2.connect(os.environ["DB_URL"]); cur = cx.cursor()
    cur.execute("""
      update commands
         set status='queued',
             leased_by=null,
             lease_at=null,
             lease_expires_at=null
       where status='leased';
    """)
    cx.commit(); cx.close()
    return jsonify({"ok": True})

@flask_app.post("/api/debug/hmac_check")
def hmac_check():
    import os, hmac, hashlib
    ok, body, provided, expected = _verify_hmac_json("EDGE_SECRET", "X-Nova-Signature")
    return jsonify({
        "ok": ok,
        "calc": expected,                 
        "provided": provided,
        "len": len(request.get_data())
    })

@flask_app.post("/api/debug/hmac_check_edge")
def hmac_check_edge():
    raw = request.get_data()
    calc = hmac.new(os.getenv("EDGE_SECRET","").encode(), raw, hashlib.sha256).hexdigest()
    return jsonify(calc=calc, len=len(raw))

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
    # Robust verify
    ok, body, provided, expected = _verify_hmac_json("EDGE_SECRET", "X-Nova-Signature")
    if not ok:
        return jsonify({"ok": False, "error": "bad signature"}), 401

    j = body
    norm = (j.get("normalized") or {})
    agent_id = body.get("agent_id")
    cmd_id = body.get("cmd_id")
    rid  = norm.get("receipt_id") or f"{agent_id}:{cmd_id}"

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
import telemetry_api
flask_app.register_blueprint(telemetry_api.bp)
flask_app.register_blueprint(bp_telemetry)
flask_app.register_blueprint(ops_bp, url_prefix="/api")

# --- Start Nova loops when the web app loads (once) -------------------------
try:
    from main import boot as _nova_boot
    _ = _nova_boot()  # returns True on success
except Exception as e:
    log.warning("Nova boot degraded: %s", e)
  
# Try to start the background Sheets flusher
try:
    start_background_flusher()
    print("[SheetsGateway] background flusher started", flush=True)
except Exception as e:
    print(f"[SheetsGateway] flusher not started: {e}", flush=True)
  
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

try:
    from phase25_decision_only import start_phase25_background_loop
    start_phase25_background_loop()
except Exception:
    pass

try:
    from phase25_planning_only import start_phase25b_background_loop
    start_phase25b_background_loop()
except Exception:
    pass

try:
    from phase25_gated_enqueue import start_phase25c_background_loop
    start_phase25c_background_loop()
except Exception:
    pass

# --- Sheets helpers ---------------------------------------------------------
def _get_gspread():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    svc_json = os.environ.get("SVC_JSON", "sentiment-log-service.json")
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(svc_json, scopes)
    return gspread.authorize(creds)

def _find_decision_id_any(obj: Any) -> str:
    """Depth-first search for 'decision_id' inside nested dicts/lists."""
    if isinstance(obj, dict):
        v = obj.get("decision_id")
        if v:
            return str(v)
        for value in obj.values():
            found = _find_decision_id_any(value)
            if found:
                return found
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            found = _find_decision_id_any(item)
            if found:
                return found
    return ""

_DECISION_ID_RE = re.compile(r"decision_id=([0-9a-f]{32})")
_HEX32_RE = re.compile(r"\b[0-9a-f]{32}\b")

def _extract_decision_id_any(obj: Any, _depth: int = 0, _max_depth: int = 6) -> str:
    """
    Aggressively hunt for a decision_id inside nested dicts/lists/strings.

    Priority:
      1) explicit 'decision_id' keys
      2) strings containing 'decision_id=<hex>'
      3) bare 32-char hex strings that look like decision_ids
    """
    if obj is None or _depth > _max_depth:
        return ""

    # 1) If it's a dict, check explicit key first
    if isinstance(obj, dict):
        # direct key
        if "decision_id" in obj and obj["decision_id"]:
            val = str(obj["decision_id"])
            m = _HEX32_RE.search(val)
            if m:
                return m.group(0)

        # also check common "meta" / "council_trace" wrappers
        for key in ("meta", "council", "intent", "payload"):
            if key in obj:
                v = _extract_decision_id_any(obj.get(key), _depth + 1, _max_depth)
                if v:
                    return v

        # then walk all values
        for v in obj.values():
            val = _extract_decision_id_any(v, _depth + 1, _max_depth)
            if val:
                return val
        return ""

    # 2) If it's a list/tuple, recurse into each item
    if isinstance(obj, (list, tuple, set)):
        for v in obj:
            val = _extract_decision_id_any(v, _depth + 1, _max_depth)
            if val:
                return val
        return ""

    # 3) If it's a string, look for decision_id patterns
    if isinstance(obj, str):
        m = _DECISION_ID_RE.search(obj)
        if m:
            return m.group(1)
        m2 = _HEX32_RE.search(obj)
        if m2:
            # Treat any lone 32-char hex as a decision_id candidate
            return m2.group(0)
        return ""

    # 4) Fallback: try stringifying weird objects
    try:
        s = str(obj)
    except Exception:
        return ""
    return _extract_decision_id_any(s, _depth + 1, _max_depth)


# Keep the old name so existing callers still work
def _find_decision_id_any(obj: Any) -> str:
    return _extract_decision_id_any(obj) or ""
  
# --- add near your other imports ---
import pytz
from datetime import datetime

# --- helpers: get worksheet & append row (reuse your existing sheets utils if you have them) ---
def _open_ws(gc, sheet_url: str, tab: str):
    sh = gc.open_by_url(sheet_url)
    try:
        return sh.worksheet(tab)
    except Exception:
        # optional: create if missing
        return sh.add_worksheet(title=tab, rows=2000, cols=20)

def _now_et_str():
    tz = os.getenv("SUMMARY_TZ", "America/New_York")
    now = datetime.now(pytz.timezone(tz))
    return now.strftime("%Y-%m-%d %H:%M:%S")

def _tag_decision_id(notes: str, decision_id: str) -> str:
    """
    Ensure the notes string contains 'decision_id=<id>' exactly once.
    If no decision_id is provided, returns notes unchanged.
    """
    notes = notes or ""
    decision_id = (decision_id or "").strip()
    if not decision_id:
        return notes

    tag = f"decision_id={decision_id}"

    # If we already have this exact tag, don't duplicate it.
    if tag in notes:
        return notes

    # If some *other* decision_id is present, just append the new one.
    if "decision_id=" in notes:
        return f"{notes}; {tag}"

    # No decision tag at all yet.
    if notes:
        return f"{notes}; {tag}"
    else:
        return tag

def log_trade_to_sheet(gc, sheet_url: str, command: dict, receipt: dict) -> None:
    """Append one row to Trade_Log. Never raise.

    This function is deliberately conservative: if the receipt shape is odd,
    we still try to log *something* useful, and we always try to attach
    the originating decision_id in the Notes column.
    """
    try:
        # --- Normalize inputs into dicts ------------------------------------
        if not isinstance(command, dict):
            command = {"id": getattr(command, "id", None), "intent": command or {}}

        if not isinstance(receipt, dict):
            status = getattr(receipt, "status", None)
            ok_val = getattr(receipt, "ok", None)
            if isinstance(ok_val, bool) and not status:
                status = "ok" if ok_val else "error"
            receipt = {"status": status, "ok": ok_val, "raw": receipt}

        # Many commands are stored as {"payload": {...}} only.
        payload = command.get("payload")
        intent = command.get("intent") or command.get("patched_intent") or {}

        # If there's no explicit "intent", treat payload itself as the intent.
        # If payload has an "intent" sub-dict, prefer that.
        if not intent:
            if isinstance(payload, dict):
                inner_intent = payload.get("intent")
                if isinstance(inner_intent, dict):
                    intent = inner_intent
                else:
                    intent = payload
            else:
                intent = {}

        if not isinstance(intent, dict):
            intent = {}

        # Many receipts have a `normalized` sub-dict; others don't.
        norm = receipt.get("normalized")
        if not isinstance(norm, dict):
            norm = {}

        # --- Derive core fields ---------------------------------------------
        ts_str = _now_et_str()

        venue = (
            intent.get("venue")
            or norm.get("venue")
            or receipt.get("venue")
            or ""
        )
        venue = str(venue).upper()

        symbol = (
            intent.get("symbol")
            or norm.get("symbol")
            or receipt.get("symbol")
            or ""
        )

        # Side/action in various forms.
        side = (
            intent.get("side")
            or intent.get("action")
            or norm.get("side")
            or norm.get("action")
            or receipt.get("side")
            or ""
        )
        side = str(side).upper()

        # Amount in quote terms (USD/USDT/etc).
        amt_q = (
            intent.get("amount_quote")
            or intent.get("amount_usd")
            or norm.get("amount_quote")
            or norm.get("amount_usd")
            or ""
        )

        # Executed quantity in base terms.
        exec_qty = (
            norm.get("executed_qty")
            or receipt.get("executed_qty")
            or receipt.get("amount")
            or ""
        )

        # Average price in quote per base.
        avg_px = (
            norm.get("avg_price")
            or receipt.get("avg_price")
            or receipt.get("price")
            or intent.get("price_usd")
            or ""
        )

        status = receipt.get("status")
        if not status:
            ok_val = receipt.get("ok")
            if isinstance(ok_val, bool):
                status = "ok" if ok_val else "error"
        status = status or ""

        base_notes = (
            receipt.get("message")
            or receipt.get("note")
            or ""
        )

        # If decision_id is embedded in notes text, recover it
        embedded = _extract_decision_id_any(base_notes)
        if embedded and not receipt.get("decision_id"):
            receipt["decision_id"] = embedded

        cmd_id = (
            command.get("id")
            or command.get("cmd_id")
            or (payload or {}).get("cmd_id")
            or ""
        )
        rcpt_id = (
            receipt.get("id")
            or receipt.get("receipt_id")
            or ""
        )

        # --- Attach decision_id in Notes ------------------------------------
        decision_id = (
            command.get("decision_id")
            or _find_decision_id_any(intent)
            or _find_decision_id_any(receipt)
            or _find_decision_id_any(command)
            or str(command.get("decision_id") or "")
        )

        notes = _tag_decision_id(base_notes, decision_id)

        note = ""          # legacy free-text column
        source = "EdgeBus" # column M

        row = [
            ts_str,   # Timestamp
            venue,    # Venue
            symbol,   # Symbol
            side,     # Side
            amt_q,    # Amount_Quote
            exec_qty, # Executed_Qty
            avg_px,   # Avg_Price
            status,   # Status
            notes,    # Notes (with decision_id=...)
            cmd_id,   # Cmd_ID
            rcpt_id,  # Receipt_ID
            note,     # Note (legacy)
            source,   # Source
        ]

        ws = _open_ws(gc, sheet_url, "Trade_Log")
        ws.append_row(row, value_input_option="USER_ENTERED")

    except Exception as e:
        # Non-fatal: we never want trading to fail because Sheets logging failed.
        log.error("bus: trade_log append failed (non-fatal): %s", e)

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
        return jsonify(ok=True, test_id=test_id, queue=q, db="postgres"), 200
    except Exception as e:
        log.warning("selftest failed: %s", e)
        return jsonify(ok=False, error=str(e), db="postgres"), 200

@flask_app.route("/api/autonomy/status", methods=["GET"])
def autonomy_status():
    """
    Lightweight JSON snapshot of the current autonomy state.

    Example response:
      {
        "mode": "AUTO_WITH_BRAKES",
        "edge_mode": "dryrun",
        "holds": { "cloud": false, "edge": false, "nova": false },
        "switches": { "nt_enqueue_live": true, "auto_enable_kraken": false },
        "limits": { "canary_max_usd": 11.0, "quote_floors": {...} }
      }
    """
    state = get_autonomy_state()
    return jsonify(state), 200

# ========== ASGI ==========
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; using WSGI: %s", e)
    app = flask_app  # type: ignore
