# wsgi.py — production entrypoint for Render/Gunicorn (bind fast, boot async)
import threading

# 1) Import the real Flask app immediately so Gunicorn can bind the port
try:
    from telegram_webhook import telegram_app as app
except Exception as e:
    # Fallback app so Render always sees a bound port
    from flask import Flask, jsonify
    app = Flask(__name__)
    @app.get("/health")
    def _health_fallback():
        return jsonify(ok=True, fallback=True, reason=str(e)), 200

# 2) Kick Nova boot in the background (don’t block worker init/bind)
def _do_boot():
    try:
        from main import boot
        boot()
    except Exception as e:
        try:
            # Log somewhere visible
            print(f"[BOOT/ASYNC] boot() failed: {e}")
        except Exception:
            pass

threading.Thread(target=_do_boot, daemon=True).start()

# 3) (Optional) lightweight health if your webhook module didn’t add one
try:
    from flask import jsonify
    @app.get("/health")
    def _health():
        return jsonify(ok=True), 200
except Exception:
    pass
