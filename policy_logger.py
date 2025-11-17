# policy_logger.py — C-Series aligned Policy_Log writer
from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, Optional

SHEET_URL = os.getenv("SHEET_URL")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")
LOG_ENABLED = os.getenv("POLICY_LOG_ENABLE", "1").lower() in ("1", "true", "yes", "on")
LOCAL_FALLBACK_PATH = os.getenv("POLICY_LOG_LOCAL", "./policy_log.jsonl")
MAX_RETRIES = 2
RETRY_BASE = 0.75  # seconds

try:
    # Prefer Bus-wide Sheets + backoff helpers if available
    from utils import get_gspread_client, with_sheet_backoff, warn as _log_warn
except Exception:  # pragma: no cover - degrade gracefully when utils not present
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
        pass


def _open_sheet():
    """
    Open (or create) the Policy_Log sheet and ensure headers match:

        Timestamp | Token | Action | Amount_USD | OK | Reason | Patched |
        Venue | Quote | Liquidity | Cooldown_Min | Notes | Intent_ID |
        Symbol | Decision | Source
    """
    import gspread  # type: ignore
    from oauth2client.service_account import ServiceAccountCredentials  # type: ignore

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
    """Append a single Policy_Log row to Sheets (or raise).

    Uses the Bus token-bucket + backoff via with_sheet_backoff;
    falls back to gspread direct if utils client is unavailable.
    """
    # Prefer shared utils client when available
    if SHEET_URL and get_gspread_client is not None:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        try:
            ws = sh.worksheet(POLICY_LOG_WS)
        except Exception:
            # Create sheet and seed headers if missing
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
        # Fallback to local gspread auth helper
        ws = _open_sheet()

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
        # Older gspread versions may not support value_input_option here
        ws.append_row(values)


def log_decision(decision: Any, intent: Dict[str, Any], when: Optional[str] = None) -> None:
    if not LOG_ENABLED:
        return

    ts = when or _ts()

    # -------- field extraction ----------
    token = (
        intent.get("token")
        or intent.get("asset")
        or intent.get("base")
        or ""
    ).upper()

    action = (intent.get("action") or intent.get("side") or "").upper()

    # Prefer patched USD amount if present, else original intent
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

    flags = decision.get("flags") or []
    notes = ""
    if flags:
        notes = ",".join(sorted(str(f) for f in flags))

    intent_id = (
        intent.get("id")
        or intent.get("intent_id")
        or intent.get("order_id")
        or ""
    )

    symbol = (
        intent.get("symbol")
        or (f"{token}/{quote}" if token and quote else token)
    )

    source = intent.get("source") or ""

        # Append to Sheets if possible; otherwise fall back to local JSONL
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

    # Always attempt local append as a safety net
    try:
        _append_local(row_dict)
    except Exception:
        # Local logging must never break the flow
        pass

    if not SHEET_URL:
        # No sheet configured; local log is all we can do.
        return

    try:
        ws = _open_sheet()
        row = [row_dict.get(h, "") for h in headers]
        # Use USER_ENTERED so numbers/JSON are interpreted sensibly
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        # If Sheets fails (quota, auth, etc.), we’ve already written to local file.
        return
