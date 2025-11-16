# sheets_bp.py
from __future__ import annotations
from flask import Blueprint, jsonify, request
import threading
from sheets_gateway import build_gateway_from_env

SHEETS_ROUTES = Blueprint("sheets", __name__)
_gateway = build_gateway_from_env()
SHEETS_ROUTES.url_prefix = "/sheets"
app.register_blueprint(SHEETS_ROUTES, url_prefix="/sheets")

_bg_thread = None
_bg_stop = threading.Event()

def start_background_flusher():
    global _bg_thread
    if _bg_thread is not None and _bg_thread.is_alive():
        return
    iv = _gateway.flush_interval
    def _run():
        import time
        while not _bg_stop.is_set():
            try:
                _gateway.flush()
            except Exception as e:
                print(f"[SheetsFlusher] error during flush: {e}", flush=True)
            _bg_stop.wait(iv)
    _bg_thread = threading.Thread(target=_run, name="SheetsFlusher", daemon=True)
    _bg_thread.start()

@SHEETS_ROUTES.route("/health", methods=["GET"])
def sheets_health():
    return jsonify({"ok": True, "health": _gateway.health()})

@SHEETS_ROUTES.route("/read", methods=["GET"])
def sheets_read():
    a1 = request.args.get("range")
    if not a1:
        return jsonify({"ok": False, "error": "missing 'range' query param"}), 400
    try:
        val = _gateway.read(a1)
        return jsonify({"ok": True, "range": a1, "values": val})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@SHEETS_ROUTES.route("/enqueue", methods=["POST"])
def sheets_enqueue():
    j = request.get_json(silent=True) or {}
    a1 = j.get("range")
    values = j.get("values")
    if not a1 or values is None:
        return jsonify({"ok": False, "error": "JSON needs 'range' and 'values'"}), 400
    _gateway.enqueue_write(a1, values)
    return jsonify({"ok": True, "queued": _gateway.queue_depth()})

@SHEETS_ROUTES.route("/flush", methods=["POST"])
def sheets_flush():
    try:
        res = _gateway.flush()
        return jsonify(res), (200 if res.get("ok") else 429)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
