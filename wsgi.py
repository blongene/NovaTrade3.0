# wsgi.py — NovaTrade Bus (quiet, ASGI-ready, no duplicate routes)

from __future__ import annotations
import os, logging, threading, time
from typing import Optional, Dict, Any
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
LOG_LEVEL = os.environ.get("NOVA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bus")

werk = logging.getLogger("werkzeug")
if LOG_LEVEL not in ("DEBUG",):
    werk.setLevel(logging.WARNING)

# ---------------------------------------------------------------------
# App (Flask WSGI, later adapted to ASGI)
# ---------------------------------------------------------------------
flask_app = Flask(__name__)

# ---------------------------------------------------------------------
# Optional: Telegram webhook integration
#   Set ENABLE_TELEGRAM=true and provide a module exposing:
#     telegram_app : flask.Blueprint
#     set_telegram_webhook() : callable (optional)
# ---------------------------------------------------------------------
def _maybe_init_telegram(app: Flask) -> Optional[str]:
    if os.environ.get("ENABLE_TELEGRAM", "").lower() not in ("1", "true", "yes"):
        return None
    try:
        from telegram_webhook import telegram_app, set_telegram_webhook  # type: ignore
    except Exception as e:
        log.warning("Telegram init failed (import): %s", e)
        return str(e)

    try:
        # Correct API: register_blueprint
        app.register_blueprint(telegram_app, url_prefix="/tg")  # type: ignore
    except Exception as e:
        log.warning("Telegram blueprint mount failed: %s", e)
        return str(e)

    # Try to set webhook, but don't fail if it errors
    try:
        if callable(set_telegram_webhook):
            set_telegram_webhook()
    except Exception as e:
        log.info("Telegram webhook degraded: %s", e)

    log.info("Telegram webhook mounted at /tg")
    return None

telegram_status = _maybe_init_telegram(flask_app)

# ---------------------------------------------------------------------
# Basic service / health
# ---------------------------------------------------------------------
@flask_app.get("/")
def index():
    return jsonify(ok=True, service="NovaTrade Bus", status="ready"), 200

@flask_app.get("/healthz")
def healthz():
    info: Dict[str, Any] = {"ok": True, "web": "up"}
    info["telegram"] = {"status": "ok" if not telegram_status else "degraded", "reason": telegram_status}  # type: ignore
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
    return jsonify(info), 200

@flask_app.get("/readyz")
def readyz():
    return jsonify(ok=True, service="Bus", ready=True), 200

# ---------------------------------------------------------------------
# EDGE AGENT API ENDPOINTS (final, hardened, single definition each)
# ---------------------------------------------------------------------
_last_telemetry = {"agent_id": None, "flat": {}, "by_venue": {}}

def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _normalize_balances(raw) -> tuple[dict, dict]:
    """
    Returns (flat_tokens, by_venue) from raw balances which may be:
      - flat tokens: {"USDC": 12.3, "BTC": 0.001}
      - nested by venue: {"COINBASE": {"USDC": 19.3, ...}, "KRAKEN": {...}}
    """
    if not isinstance(raw, dict):
        return {}, {}

    # Detect nested (venue -> tokens)
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
        # Already flat token map
        flat = {t: round(_safe_float(a), 8) for t, a in raw.items()}
        return flat, {}

@flask_app.post("/api/telemetry/push")
def api_telemetry_push():
    """
    Edge Agent posts wallet snapshots/telemetry.
    Accepts:
      {"agent_id": "...", "balances": { ... flat or nested ... }, ...}
    """
    data = request.get_json(silent=True) or {}
    agent_id = data.get("agent_id", "edge")
    raw_balances = data.get("balances") or {}

    flat, by_venue = _normalize_balances(raw_balances)

    # Store last snapshot (in-memory)
    _last_telemetry["agent_id"] = agent_id
    _last_telemetry["flat"] = flat
    _last_telemetry["by_venue"] = by_venue

    # Concise log: per-venue token counts or flat summary
    if by_venue:
        venue_counts = {v: len(tokens) for v, tokens in by_venue.items()}
        log.info("📡 Telemetry from %s: venues=%s | flat_tokens=%d",
                 agent_id, venue_counts, len(flat))
    else:
        # Log up to a few tokens to keep noise low
        preview = dict(list(flat.items())[:4])
        log.info("📡 Telemetry from %s: %s%s",
                 agent_id, preview, " …" if len(flat) > 4 else "")

    return jsonify(ok=True, received=(len(by_venue) or len(flat))), 200

# Legacy aliases → same handler
@flask_app.post("/api/telemetry/push_balances")
@flask_app.post("/api/edge/balances")
@flask_app.post("/bus/push_balances")
def api_telemetry_push_aliases():
    return api_telemetry_push()

@flask_app.get("/api/telemetry/last")
def api_telemetry_last():
    """Debug endpoint to view last normalized snapshot (flat + by_venue)."""
    return jsonify(ok=True, **_last_telemetry), 200

@flask_app.post("/api/commands/pull")
def api_commands_pull():
    log.debug("🪙 Edge poll → ok (empty queue)")
    return jsonify(ok=True, commands=[]), 200

@flask_app.post("/api/commands/ack")
def api_commands_ack():
    data = request.get_json(silent=True) or {}
    log.info("✅ ACK from %s → %s (%s)",
             data.get("agent_id", "edge"),
             data.get("command_id", "?"),
             data.get("status", "ok"))
    return jsonify(ok=True), 200

@flask_app.post("/api/heartbeat")
def api_heartbeat():
    return jsonify(ok=True, service="Bus", alive=True), 200

# ---------------------------------------------------------------------
# Quiet JSON error handlers (single definitions)
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Optional: Receipts bridge (background, quiet)
# ---------------------------------------------------------------------
def _start_receipts_bridge():
    if os.environ.get("DISABLE_RECEIPTS_BRIDGE", "").lower() in ("1", "true", "yes"):
        return
    try:
        import receipts_bridge  # type: ignore
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

# Optional: tiny “I’m alive” scheduler marker (no jobs here)
def _scheduler_banner():
    log.info("⏰ Scheduler thread active.")
threading.Thread(target=_scheduler_banner, daemon=True).start()

# ---------------------------------------------------------------------
# ASGI adapter for Uvicorn
# ---------------------------------------------------------------------
try:
    from asgiref.wsgi import WsgiToAsgi
    app = WsgiToAsgi(flask_app)
except Exception as e:
    log.warning("ASGI adapter unavailable; falling back to WSGI: %s", e)
    app = flask_app  # type: ignore
