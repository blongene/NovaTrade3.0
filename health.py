# health.py
import os, time
from flask import Blueprint, jsonify
bp_health = Blueprint("health", __name__)
_started = int(time.time())

@bp_health.get("/health")
def health():
    return jsonify(ok=True, uptime_s=int(time.time() - _started),
                   commit=os.getenv("RENDER_GIT_COMMIT","")[:8])
