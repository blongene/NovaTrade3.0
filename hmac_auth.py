# hmac_auth.py — robust HMAC checker used by /api/commands and /ops/*
import os, time, hmac, hashlib, json

# Secret: from env or an optional file path (no 'cat' helper needed)
OUTBOX_SECRET = os.getenv("OUTBOX_SECRET") or ""
SECRET_FILE = os.getenv("OUTBOX_SECRET_FILE")
if not OUTBOX_SECRET and SECRET_FILE and os.path.exists(SECRET_FILE):
    try:
        with open(SECRET_FILE, "r", encoding="utf-8") as f:
            OUTBOX_SECRET = f.read().strip()
    except Exception:
        OUTBOX_SECRET = ""

SECRET_BYTES = OUTBOX_SECRET.encode()

# Timestamp skew (ms). If 0, skip ts checks.
MAX_SKEW_MS = int(os.getenv("HMAC_MAX_SKEW_MS", "300000"))  # 5 minutes default

def _h(body_raw: bytes) -> str:
    return hmac.new(SECRET_BYTES, body_raw, hashlib.sha256).hexdigest()

def _h_combo(ts: str, body_raw: bytes, sep: str = "") -> str:
    if sep:
        msg = (ts + sep + body_raw.decode()).encode()
    else:
        msg = (ts + body_raw.decode()).encode()
    return hmac.new(SECRET_BYTES, msg, hashlib.sha256).hexdigest()

def _get_headers(req):
    return (req.headers.get("X-Timestamp", ""), req.headers.get("X-Signature", ""))

def _ts_ok(ts: str) -> bool:
    if not MAX_SKEW_MS:
        return True
    try:
        client_ms = int(ts)
        now_ms = int(time.time() * 1000)
        return abs(now_ms - client_ms) <= MAX_SKEW_MS
    except Exception:
        return False

def check_hmac(request):
    """
    Returns (ok: bool, err: str). Accepts common signing variants:
      1) HMAC(secret, raw_body)
      2) HMAC(secret, ts + raw_body)
      3) HMAC(secret, raw_body + ts)
      4) HMAC(secret, ts + ':' + raw_body)
      5) HMAC(secret, ts + '.' + raw_body)
      6) HMAC(secret, ts + '\\n' + raw_body)
    """
    if not SECRET_BYTES:
        return (False, "OUTBOX_SECRET missing")

    ts, sig = _get_headers(request)
    try:
        raw = request.get_data()  # <- keep default cache=True so the view can get_json() later
    except Exception:
        raw = b""

    if ts and not _ts_ok(ts):
        return (False, "timestamp out of range")

    trials = [
        _h(raw),
        _h_combo(ts, raw, ""),
        _h_combo("", raw, ""),              # raw + ts not deterministic w/out delim; skip
        _h_combo(ts, raw, ":"),
        _h_combo(ts, raw, "."),
        _h_combo(ts, raw, "\n"),
    ]
    if sig and any(hmac.compare_digest(sig, t) for t in trials):
        return (True, "")
    return (False, "invalid HMAC")

def require_hmac(request):
    ok, err = check_hmac(request)
    return ok, err
# Accept both header names
def _get_headers(req):
    ts = req.headers.get("X-Timestamp", "")
    sig = req.headers.get("X-Signature") or req.headers.get("X-Nova-Signature") or ""
    return (ts, sig)

# Allow EDGE_SECRET as a fallback (optional)
EDGE_SECRET = os.getenv("EDGE_SECRET") or ""
if not OUTBOX_SECRET and EDGE_SECRET:
    OUTBOX_SECRET = EDGE_SECRET
SECRET_BYTES = OUTBOX_SECRET.encode()
