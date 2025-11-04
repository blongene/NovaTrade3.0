
# policy_logger.py — NovaTrade policy decision logger (Sheets + local fallback)
# - Backward compatible with previous log_policy_decision(...)
# - Adds log_decision(decision=..., intent=..., when=...) expected by the Bus
# - Resilient to 429s with retries; never blocks your main flow
from __future__ import annotations
import os, time, json, traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

# -----------------------------
# Configuration
# -----------------------------
SHEET_URL     = os.getenv("SHEET_URL")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")
LOG_ENABLED   = os.getenv("POLICY_LOG_ENABLE","1").lower() in ("1","true","yes")
CREDS_HINTS   = (
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CREDS_JSON_PATH",
    "SVC_JSON",
)
CREDS_FALLBACKS = (
    "/etc/secrets/sentiment-log-service.json",
    "sentiment-log-service.json",
)

LOCAL_FALLBACK_PATH = os.getenv("POLICY_LOG_LOCAL","./policy_log.jsonl")
MAX_RETRIES = 3
RETRY_BASE  = 0.75  # seconds

# -----------------------------
# Helpers
# -----------------------------
def _ts_human(dt: Optional[datetime]=None) -> str:
    return (dt or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")

def _creds_path() -> Optional[str]:
    for k in CREDS_HINTS:
        v = os.getenv(k)
        if v and os.path.exists(v):
            return v
    for v in CREDS_FALLBACKS:
        if os.path.exists(v):
            return v
    return None

def _open_sheet():
    """Return (gspread_client, spreadsheet_handle, worksheet_handle)."""
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    svc = _creds_path()
    if not svc:
        raise FileNotFoundError("Google creds JSON not found. Set GOOGLE_APPLICATION_CREDENTIALS or SVC_JSON.")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
    gc = gspread.authorize(creds)
    if not SHEET_URL:
        raise ValueError("SHEET_URL is not set")
    sh = gc.open_by_url(SHEET_URL)
    try:
        ws = sh.worksheet(POLICY_LOG_WS)
    except Exception:
        ws = sh.add_worksheet(title=POLICY_LOG_WS, rows=4000, cols=20)
        ws.append_row(
            ["Timestamp","Intent_ID","Agent_Target","Venue","Symbol","Side",
             "Amount","Flags","Allowed","Reason","Patched_JSON","Decision_JSON","Source"],
            value_input_option="USER_ENTERED"
        )
    return gc, sh, ws

def _safe_amount(intent: Dict[str, Any]) -> Any:
    for k in ("amount","amount_usd","amount_quote","quote_amount","qty","size"):
        if k in intent:
            return intent.get(k)
    return ""

def _safe_str(x: Any) -> str:
    try:
        if x is None: return ""
        return str(x)
    except Exception:
        return ""

def _to_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",",":"), ensure_ascii=False)
    except Exception:
        try:
            return json.dumps({"repr": repr(obj)}, separators=(",",":"))
        except Exception:
            return "{}"

def _extract_decision(decision: Any) -> Dict[str, Any]:
    """Normalize various decision shapes into a dict."""
    if isinstance(decision, dict):
        allowed = bool(decision.get("allowed", True))
        reason  = _safe_str(decision.get("reason",""))
        flags   = decision.get("flags") or []
        patched = decision.get("patched") or {}
        return {"allowed": allowed, "reason": reason, "flags": flags, "patched": patched}
    # string-based legacy: 'pass'|'block'|'hold' with optional reasons
    s = _safe_str(decision).lower()
    if s in ("pass","allow","ok","true","yes"):
        return {"allowed": True, "reason": "", "flags": [], "patched": {}}
    if s in ("block","deny","false","no"):
        return {"allowed": False, "reason": "", "flags": [], "patched": {}}
    if s in ("hold","warn"):
        return {"allowed": True, "reason": "warn", "flags": ["policy_warn"], "patched": {}}
    # default permissive
    return {"allowed": True, "reason": _safe_str(decision), "flags": [], "patched": {}}

def _append_local_fallback(row: Dict[str, Any]) -> None:
    try:
        with open(LOCAL_FALLBACK_PATH, "a", encoding="utf-8") as f:
            f.write(_to_json(row) + "\n")
    except Exception:
        # Never throw
        pass

# -----------------------------
# Public API
# -----------------------------
def log_decision(decision: Any, intent: Dict[str, Any], when: Optional[str]=None) -> None:
    """
    Primary entry point used by the Bus.
      - decision: dict with keys {allowed, reason, flags, patched} or legacy strings
      - intent:   original (possibly patched) intent dict
      - when:     optional ISO or human TS; defaults to now()
    Resilient: never raises; on failure writes to LOCAL_FALLBACK_PATH.
    """
    if not LOG_ENABLED:
        return

    # Normalize input
    norm = _extract_decision(decision)
    # Prefer supplied timestamp; else now() in human format
    ts = when or _ts_human()

    row = {
        "Timestamp": ts,
        "Intent_ID": _safe_str(intent.get("id") or intent.get("client_id") or ""),
        "Agent_Target": _safe_str(intent.get("agent_target") or ""),
        "Venue": _safe_str(intent.get("venue") or intent.get("venue_id") or "").upper(),
        "Symbol": _safe_str(intent.get("symbol") or intent.get("product_id") or ""),
        "Side": _safe_str(intent.get("side") or intent.get("action") or "").upper(),
        "Amount": _safe_amount(intent),
        "Flags": ",".join([_safe_str(f) for f in (norm.get("flags") or [])]),
        "Allowed": "YES" if norm.get("allowed", True) else "NO",
        "Reason": _safe_str(norm.get("reason","")),
        "Patched_JSON": _to_json(norm.get("patched") or {}),
        "Decision_JSON": _to_json(decision),
        "Source": _safe_str(intent.get("source") or ""),
    }

    # Attempt Sheets with retries; fallback to local
    try:
        import gspread  # lazy import to avoid hard dependency at module import
        from oauth2client.service_account import ServiceAccountCredentials  # noqa: F401  # imported by _open_sheet
    except Exception:
        _append_local_fallback(row)
        return

    if not SHEET_URL:
        _append_local_fallback(row)
        return

    delay = RETRY_BASE
    for attempt in range(1, MAX_RETRIES+1):
        try:
            _, _, ws = _open_sheet()
            # Order must match header. Append as list to keep alignment.
            values = [
                row["Timestamp"], row["Intent_ID"], row["Agent_Target"], row["Venue"], row["Symbol"],
                row["Side"], row["Amount"], row["Flags"], row["Allowed"], row["Reason"],
                row["Patched_JSON"], row["Decision_JSON"], row["Source"]
            ]
            ws.append_row(values, value_input_option="USER_ENTERED")
            return
        except Exception as e:
            # 429s or transient network errors; retry with backoff, then fall back
            if attempt >= MAX_RETRIES:
                _append_local_fallback({"error": str(e), **row})
                return
            time.sleep(delay)
            delay *= 2.0

# -----------------------------
# Backward-compat shim
# -----------------------------
def log_policy_decision(intent: dict, decision: str, reasons: List[str]):
    """
    Legacy API kept for compatibility with older modules.
    Maps to log_decision with a normalized decision object.
    """
    dec = {"allowed": decision.lower() in ("pass","allow","ok","true","yes"),
           "reason": "; ".join(reasons or []),
           "flags": [],
           "patched": {}}
    log_decision(decision=dec, intent=intent, when=None)
