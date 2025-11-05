# policy_logger.py — robust policy logging (Sheets or local JSONL)
from __future__ import annotations
import os, time, json
from datetime import datetime
from typing import Any, Dict, List, Optional

SHEET_URL           = os.getenv("SHEET_URL")
POLICY_LOG_WS       = os.getenv("POLICY_LOG_WS", "Policy_Log")
LOG_ENABLED         = os.getenv("POLICY_LOG_ENABLE","1").lower() in ("1","true","yes","on")
LOCAL_FALLBACK_PATH = os.getenv("POLICY_LOG_LOCAL","./policy_log.jsonl")
MAX_RETRIES         = 2
RETRY_BASE          = 0.75

def _ts(dt: Optional[datetime]=None) -> str:
    return (dt or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")

def _to_json(obj: Any) -> str:
    try: return json.dumps(obj, separators=(",",":"), ensure_ascii=False)
    except Exception: return "{}"

def _append_local(row: Dict[str, Any]) -> None:
    try:
        with open(LOCAL_FALLBACK_PATH, "a", encoding="utf-8") as f:
            f.write(_to_json(row) + "\n")
    except Exception:
        pass

def _open_sheet():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    svc = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
           or os.getenv("SVC_JSON")
           or "sentiment-log-service.json")
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)
    try:
        ws = sh.worksheet(POLICY_LOG_WS)
    except Exception:
        ws = sh.add_worksheet(title=POLICY_LOG_WS, rows=4000, cols=20)
        ws.append_row(
            ["Timestamp","Intent_ID","Agent_Target","Venue","Symbol","Side",
             "Amount","Flags","Allowed","Reason","Patched_JSON","Decision_JSON","Source","Policy_ID"],
            value_input_option="USER_ENTERED")
    return ws

def log_decision(decision: Any, intent: Dict[str, Any], when: Optional[str]=None) -> None:
    if not LOG_ENABLED: return
    ts = when or _ts()
    ok = decision.get("ok")
    allowed = decision.get("allowed")
    allowed_norm = bool(allowed if allowed is not None else ok if ok is not None else True)
    row = {
        "Timestamp": ts,
        "Intent_ID": str(intent.get("id","")),
        "Agent_Target": str(intent.get("agent_target","")),
        "Venue": str(intent.get("venue","")).upper(),
        "Symbol": str(intent.get("symbol","")),
        "Side": str(intent.get("side","")).upper(),
        "Amount": intent.get("amount",""),
        "Flags": ",".join([str(f) for f in (decision.get("flags") or [])]),
        "Allowed": "YES" if allowed_norm else "NO",
        "Reason": decision.get("reason",""),
        "Patched_JSON": _to_json(decision.get("patched_intent") or decision.get("patched") or {}),
        "Decision_JSON": _to_json(decision),
        "Source": intent.get("source",""),
        "Policy_ID": decision.get("policy_id",""),
    }
    try:
        import gspread  # noqa
    except Exception:
        _append_local(row)
        return
    if not SHEET_URL:
        _append_local(row)
        return
    delay = RETRY_BASE
    for attempt in range(MAX_RETRIES):
        try:
            ws = _open_sheet()
            ws.append_row(list(row.values()), value_input_option="USER_ENTERED")
            return
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                _append_local({"error": str(e), **row})
                return
            time.sleep(delay)
            delay *= 2

def log_policy_decision(intent: dict, decision: str, reasons: List[str]):
    dec = {"ok": decision.lower() in ("pass","allow","ok","true","yes"),
           "reason": "; ".join(reasons or []),
           "flags": [], "patched_intent": {}}
    log_decision(decision=dec, intent=intent)
