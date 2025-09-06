cat > hmac_auth.py <<'PY'
import os, hmac, hashlib
from flask import request

OUTBOX_SECRET = os.environ.get("OUTBOX_SECRET", "")

def _hex_hmac(body: bytes) -> str:
    return hmac.new(OUTBOX_SECRET.encode(), body, hashlib.sha256).hexdigest()

def require_hmac(req=None):
    """
    Verify HMAC against the *raw* body bytes exactly as received.
    Accept header names:
      - X-Signature (hex digest)
      - X-Hub-Signature-256: 'sha256=<hexdigest>' (GitHub style)
    Also allow an emergency bypass via OUTBOX_ALLOW_UNAUTH=1 (for debugging only).
    """
    if os.environ.get("OUTBOX_ALLOW_UNAUTH", "0").lower() in {"1","true","yes"}:
        return True, "bypass"

    if not OUTBOX_SECRET:
        return False, "server missing OUTBOX_SECRET"

    req = req or request
    body = req.get_data(cache=False) or b""
    hdr  = (req.headers.get("X-Signature") or
            req.headers.get("X-Signature-Hex") or
            req.headers.get("X-Hub-Signature-256") or "")

    if hdr.startswith("sha256="):
        hdr = hdr.split("=",1)[1]

    expected = _hex_hmac(body)
    ok = hmac.compare_digest(hdr, expected)
    return ok, ("ok" if ok else "unauthorized")
PY
