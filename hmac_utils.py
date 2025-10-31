# hmac_utils.py
import os, hmac, hashlib, json

SECRET = os.getenv("OUTBOX_SECRET", "")

def canonical(body: dict) -> bytes:
  # Sort keys for a stable signature surface
  return json.dumps(body, separators=(",",":"), sort_keys=True).encode("utf-8")

def sign(body: dict) -> str:
  if not SECRET:
    return ""
  return hmac.new(SECRET.encode("utf-8"), canonical(body), hashlib.sha256).hexdigest()

def verify(body: dict, provided_sig: str) -> bool:
  if not SECRET:
    # If no secret configured, accept (useful in dev/private hosts)
    return True
  expected = sign(body)
  try:
    return hmac.compare_digest(expected, provided_sig or "")
  except Exception:
    return False
