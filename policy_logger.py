from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, Optional
import time
from insight_model import CouncilInsight

SHEET_URL = os.getenv("SHEET_URL")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")
LOG_ENABLED = os.getenv("POLICY_LOG_ENABLE", "1").lower() in ("1", "true", "yes", "on")
LOCAL_FALLBACK_PATH = os.getenv("POLICY_LOG_LOCAL", "./policy_log.jsonl")
INSIGHT_LOG_PATH = os.getenv("COUNCIL_INSIGHT_LOG", "council_insights.jsonl")
COUNCIL_INSIGHT_LOG = os.environ.get("COUNCIL_INSIGHT_LOG", "council_insights.jsonl")
COUNCIL_INSIGHTS_FILE = os.environ.get(
    "COUNCIL_INSIGHTS_FILE", "council_insights.jsonl"
)

try:
    # Prefer Bus-wide Sheets helpers if available
    from utils import get_gspread_client, with_sheet_backoff, warn as _log_warn
except Exception:
    get_gspread_client = None

    def with_sheet_backoff(fn):
        return fn

    def _log_warn(msg: str) -> None:
        try:
            print(f"[policy_logger] {msg}")
        except Exception:
            pass


def _ts(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")


def _to_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _append_local(row: Dict[str, Any]) -> None:
    try:
        with open(LOCAL_FALLBACK_PATH, "a", encoding="utf-8") as f:
            f.write(_to_json(row) + "\n")
    except Exception:
        # Local logging must never break the policy flow
        pass


def _open_sheet_legacy():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    svc = (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("GOOGLE_CREDENTIALS_JSON")
        or os.getenv("SVC_JSON")
        or "sentiment-log-service.json"
    )

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
            [
                "Timestamp",
                "Token",
                "Action",
                "Amount_USD",
                "OK",
                "Reason",
                "Patched",
                "Venue",
                "Quote",
                "Liquidity",
                "Cooldown_Min",
                "Notes",
                "Intent_ID",
                "Symbol",
                "Decision",
                "Source",
            ],
            value_input_option="USER_ENTERED",
        )
    return ws


@with_sheet_backoff
def _append_sheet_row(row: Dict[str, Any]) -> None:
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL not configured")

    if get_gspread_client is not None:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        try:
            ws = sh.worksheet(POLICY_LOG_WS)
        except Exception:
            ws = sh.add_worksheet(title=POLICY_LOG_WS, rows=4000, cols=20)
            ws.append_row(
                [
                    "Timestamp",
                    "Token",
                    "Action",
                    "Amount_USD",
                    "OK",
                    "Reason",
                    "Patched",
                    "Venue",
                    "Quote",
                    "Liquidity",
                    "Cooldown_Min",
                    "Notes",
                    "Intent_ID",
                    "Symbol",
                    "Decision",
                    "Source",
                ],
                value_input_option="USER_ENTERED",
            )
    else:
        # Fallback to direct gspread auth
        ws = _open_sheet_legacy()

    headers = [
        "Timestamp",
        "Token",
        "Action",
        "Amount_USD",
        "OK",
        "Reason",
        "Patched",
        "Venue",
        "Quote",
        "Liquidity",
        "Cooldown_Min",
        "Notes",
        "Intent_ID",
        "Symbol",
        "Decision",
        "Source",
    ]
    values = [row.get(h, "") for h in headers]
    try:
        ws.append_row(values, value_input_option="USER_ENTERED")
    except TypeError:
        ws.append_row(values)

def _append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    """
    Append a single JSON object as one line to a JSONL file.
    Best-effort; swallow any file IO errors.
    """
    try:
        # Ensure directory exists if path includes one
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")
    except Exception as e:
        try:
            warn(f"policy_logger: failed to append JSONL {path}: {e}")
        except Exception:
            pass

def log_decision(decision: Any, intent: Dict[str, Any], when: Optional[str] = None) -> None:
    """
    Log a policy decision both locally (JSONL) and to the Policy_Log sheet.

    This is tolerant of different decision shapes:

      * Trade guard decisions:
          {
            "ok": bool,
            "status": "...",
            "reason": "...",
            "intent": {...},
            "patched": {...},
            "decision_id": "...",
            "meta": {...},
            ...
          }

      * Manual policy decisions:
          {
            "ok": bool,
            "reason": "...",
            "patched_intent": {...},
            ...
          }

    We always:
      - Record the full decision JSON in the 'Decision' column.
      - Derive a human-friendly snapshot in the core columns.
      - If decision_id is present, include `decision_id=<id>` in Notes.
    """
    if not LOG_ENABLED:
        return

    ts = when or _ts()

    token = (
        intent.get("token")
        or intent.get("asset")
        or intent.get("base")
        or ""
    ).upper()

    action = (intent.get("action") or intent.get("side") or "").upper()

    patched = decision.get("patched") or decision.get("patched_intent") or {}
    amt_usd = patched.get("amount_usd")
    if amt_usd is None:
        amt_usd = intent.get("amount_usd", intent.get("amount"))

    ok = bool(decision.get("ok", True))
    reason = decision.get("reason") or ""

    venue = (intent.get("venue") or "").upper()
    quote = (intent.get("quote") or "").upper()

    liquidity = decision.get("liquidity", "")
    cooldown_min = decision.get("cooldown_min", "")

    # Collect flags + decision_id into Notes
    flags = decision.get("flags") or []
    notes_parts = []

    if flags:
        notes_parts.extend(sorted(str(f) for f in flags))

    decision_id = decision.get("decision_id") or ""
    if decision_id:
        notes_parts.append(f"decision_id={decision_id}")

    notes = ",".join(notes_parts)

    intent_id = (
        intent.get("id")
        or intent.get("intent_id")
        or intent.get("order_id")
        or ""
    )

    symbol = intent.get("symbol") or (f"{token}/{quote}" if token and quote else token)

    source = intent.get("source") or ""

    row_dict: Dict[str, Any] = {
        "Timestamp": ts,
        "Token": token,
        "Action": action,
        "Amount_USD": amt_usd,
        "OK": "TRUE" if ok else "FALSE",
        "Reason": reason,
        "Patched": _to_json(patched),
        "Venue": venue,
        "Quote": quote,
        "Liquidity": liquidity,
        "Cooldown_Min": cooldown_min,
        "Notes": notes,
        "Intent_ID": intent_id,
        "Symbol": symbol,
        "Decision": _to_json(decision),
        "Source": source,
    }

    # Always log locally first
    _append_local(row_dict)

    # Then try Sheets if configured
    if not SHEET_URL:
        return

    try:
        _append_sheet_row(row_dict)
    except Exception as e:
        try:
            _log_warn(f"Policy_Log append failed: {e}")
        except Exception:
            pass

        # Also mirror into council_insights.jsonl for Ops API / Council_Insight sheet
    try:
        log_decision_insight(decision, intent)
    except Exception as e:
        warn(f"policy_logger: insight logging failed: {e}")
        
def log_decision_insight(decision: Dict[str, Any], intent: Dict[str, Any]) -> None:
    """
    Best-effort: append a CouncilInsight-style row to council_insights.jsonl.
    This is a small local log that ops_api / Apps Script can read. It never
    touches Sheets and must never break the trading path.
    """
    try:
        # Correlation id
        decision_id = decision.get("decision_id") or decision.get("id")
        if not decision_id:
            return

        ts = time.time()

        # Autonomy mode / state
        autonomy = (
            decision.get("autonomy")
            or decision.get("autonomy_mode")
            or decision.get("autonomy_state")
            or ""
        )

        story = decision.get("story") or ""
        ok = bool(decision.get("ok", True))
        reason = decision.get("reason") or decision.get("status") or ""

        # Flags / tags
        flags = decision.get("flags") or decision.get("applied") or []
        if isinstance(flags, str):
            flags = [flags]

        # Council trace (who influenced this)
        council = (
            decision.get("council_trace")
            or decision.get("council")
            or {}
        )

        # Intents
        raw_intent = decision.get("intent") or intent or {}
        patched_intent = (
            decision.get("patched_intent")
            or decision.get("patched")
            or {}
        )

        venue = (patched_intent.get("venue") or raw_intent.get("venue") or "").upper()
        symbol = (
            patched_intent.get("symbol")
            or raw_intent.get("symbol")
            or None
        )

        # Build the record (schema matches insight_model.CouncilInsight.to_dict())
        record = {
            "decision_id": decision_id,
            "ts": ts,
            "autonomy": autonomy,
            "council": council,
            "story": story,
            "ok": ok,
            "reason": reason,
            "flags": flags,
            "raw_intent": raw_intent,
            "patched_intent": patched_intent,
            "venue": venue,
            "symbol": symbol,
        }

        line = json.dumps(record, sort_keys=True)

        with open(INSIGHT_LOG_PATH, "a") as f:
            f.write(line + "\n")

    except Exception as e:
        # Never break the trading path for this
        try:
            _log_warn(f"CouncilInsight append failed: {e}")
        except Exception:
            pass
