# vault_rotation_gatekeeper.py

from typing import List, Dict, Any
from rotation_signal_engine import scan_rotation_candidates

# Optional: if you want a Telegram ping when a gate passes/fails
try:
    from utils import send_telegram_message as _notify
except Exception:
    def _notify(*a, **k):  # no-op fallback
        pass


def _norm(s: str) -> str:
    return (s or "").strip().upper()


def _pick_token(cands: List[Dict[str, Any]], token: str) -> List[Dict[str, Any]]:
    tu = _norm(token)
    return [c for c in cands if _norm(str(c.get("Token", ""))) == tu]


def _get_confidence(c: Dict[str, Any]) -> float:
    """
    Tries common confidence fields; falls back to 0.0 if none present.
    Accepts either 0..1 or 0..100 scales and normalizes to 0..1.
    """
    for key in ("Confidence", "confidence", "Score", "score", "SignalScore"):
        if key in c and c[key] is not None and str(c[key]).strip() != "":
            try:
                val = float(str(c[key]).replace("%", "").strip())
                # Heuristic: treat >1 as percent
                return max(0.0, min(1.0, val / 100.0 if val > 1.0 else val))
            except Exception:
                continue
    return 0.0


def gate_vault_rotation(token: str, min_confidence: float = 0.60) -> List[Dict[str, Any]]:
    """
    Gatekeeper for a single token.

    - Pulls rotation candidates from rotation_signal_engine
    - Filters to the requested token
    - Checks confidence against min_confidence (0..1)
    - Returns the (possibly filtered) candidates for downstream executors

    NOTE:
      This function is intentionally side‑effect‑free (no Sheet writes).
      Executors (e.g., vault_rotation_executor) should act on the returned list.
    """
    # inside gate_vault_rotation(...)
    try:
        _ = _load_candidates_somehow  # guard: refer but don't call if not defined
    except NameError:
        return False  # or just skip gracefully

    token = (token or "").strip()
    if not token:
        print("⚠️ gate_vault_rotation: empty token, skipping.")
        return []

    print(f"🚀 Executing vault gatekeeper for: {token}")

    # --- Get candidates, compatible with engines with/without token_override
    try:
        cands = scan_rotation_candidates(token_override=token)
    except TypeError:
        # Older signature: filter locally
        print("ℹ️ rotation_signal_engine.scan_rotation_candidates has no token_override; filtering locally.")
        cands = scan_rotation_candidates()
        cands = _pick_token(cands, token)

    # --- Filter to this token if engine returned more than one
    if not cands:
        print(f"⚠️ No rotation candidates found for {token}.")
        return []

    # Some engines might still return multi-token lists; hard filter
    cands = _pick_token(cands, token) or cands
    if not cands:
        print(f"⚠️ Candidates present but none matched {token}.")
        return []

    # --- Confidence gate
    passed: List[Dict[str, Any]] = []
    for c in cands:
        conf = _get_confidence(c)
        print(f"📊 Confidence for {token}: {conf:.2f} (threshold {min_confidence:.2f})")
        if conf >= min_confidence:
            passed.append(c)

    if passed:
        msg = f"🛡️ Vault Gate PASS: {token} ({len(passed)} cand) ≥ threshold {min_confidence:.2f}"
        print(msg)
        _notify(msg)
        return passed

    msg = f"🛡️ Vault Gate FAIL: {token} (< {min_confidence:.2f})"
    print(msg)
    _notify(msg)
    return []


__all__ = ["gate_vault_rotation"]

