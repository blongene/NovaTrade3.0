# hmac_auth.py — HMAC helpers
import hmac, hashlib, time

def sign(secret: str, body: bytes, ts: str) -> str:
    # canonical: "<unix_ts>.<raw_body>"
    msg = ts.encode() + b"." + body
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()

def verify(secret: str, body: bytes, ts: str, sig: str, ttl_s: int = 180) -> bool:
    # reject if stale or missing
    try:
        ts_i = int(ts)
    except Exception:
        return False
    if not secret or abs(time.time() - ts_i) > ttl_s:
        return False
    expected = sign(secret, body, ts)
    return hmac.compare_digest(expected, sig)
