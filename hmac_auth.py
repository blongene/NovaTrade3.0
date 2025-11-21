# hmac_auth.py — robust HMAC checker
import os, time, hmac, hashlib, json
from flask import request

OUTBOX_SECRET = os.getenv("OUTBOX_SECRET") or ""
MAX_SKEW_MS = int(os.getenv("HMAC_MAX_SKEW_MS", "300000"))

def check_hmac(req=None):
    """
    Returns (ok: bool, err: str). 
    Robustly checks Raw bytes AND Canonical JSON.
    """
    r = req or request
    if not OUTBOX_SECRET:
        return (False, "OUTBOX_SECRET missing")

    # 1. Get Signature
    sig = r.headers.get("X-Nova-Signature") or \
          r.headers.get("X-NT-Sig") or \
          r.headers.get("X-Outbox-Signature") or \
          r.headers.get("X-Signature")
    
    if not sig:
        return (False, "missing signature header")

    # 2. Timestamp Check
    ts = r.headers.get("X-Timestamp", "")
    if ts and MAX_SKEW_MS > 0:
        try:
            delta = abs(int(time.time() * 1000) - int(ts))
            if delta > MAX_SKEW_MS:
                return (False, f"timestamp skew {delta}ms > {MAX_SKEW_MS}")
        except:
            pass # malformed ts is ignored if we only care about hash

    # 3. Verify (Robust)
    raw = r.get_data() or b""
    sec_bytes = OUTBOX_SECRET.encode("utf-8")

    # A) Raw check
    if hmac.compare_digest(hmac.new(sec_bytes, raw, hashlib.sha256).hexdigest(), sig):
        return (True, "")

    # B) Canonical check (fallback)
    try:
        body = json.loads(raw.decode("utf-8"))
        # This matches the Edge's sort_keys=True
        canon = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        if hmac.compare_digest(hmac.new(sec_bytes, canon, hashlib.sha256).hexdigest(), sig):
            return (True, "")
    except:
        pass
    
    return (False, "invalid HMAC")

def require_hmac(req=None):
    return check_hmac(req)
