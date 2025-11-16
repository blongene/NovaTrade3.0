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


def log_decision(decision: Any, intent: Dict[str, Any], when: Optional[str] = None) -> None:
    """
    Canonical logger used by PolicyEngine.

    Parameters:
        decision: dict-like, should contain "ok", "reason", "patched" (or "patched_intent")
        intent:   original intent dict (token, venue, quote, amount_usd, etc.)
        when:     optional timestamp override
    """
    if not LOG_ENABLED:
        return

    ts = when or _ts()

    token = (intent.get("token") or "").upper()
    action = (intent.get("action") or intent.get("side") or "").upper()
    amount_usd = intent.get("amount_usd", intent.get("amount"))

    ok = decision.get("ok")
    allowed_norm = bool(ok if ok is not None else True)

    reason = decision.get("reason") or ""
    patched = decision.get("patched") or decision.get("patched_intent") or {}
    venue = (intent.get("venue") or "").upper()
    quote = (intent.get("quote") or "").upper()

    liquidity = decision.get("liquidity", "")
    cooldown_min = decision.get("cooldown_min", "")
    flags = decision.get("flags") or []

    notes = ""
    if flags:
        notes = ",".join(sorted(set(str(f) for f in flags)))

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

    row_dict: Dict[str, Any] = {
        "Timestamp": ts,
        "Token": token,
        "Action": action,
        "Amount_USD": amount_usd,
        "OK": "TRUE" if allowed_norm else "FALSE",
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

    # Try Sheets; fall back to local JSONL if anything goes wrong.
    try:
        import gspread  # noqa: F401
    except Exception:
        _append_local(row_dict)
        return

    if not SHEET_URL:
        _append_local(row_dict)
        return

    delay = RETRY_BASE
    for attempt in range(MAX_RETRIES):
        try:
            ws = _open_sheet()
            ws.append_row(
                [
                    row_dict["Timestamp"],
                    row_dict["Token"],
                    row_dict["Action"],
                    row_dict["Amount_USD"],
                    row_dict["OK"],
                    row_dict["Reason"],
                    row_dict["Patched"],
                    row_dict["Venue"],
                    row_dict["Quote"],
                    row_dict["Liquidity"],
                    row_dict["Cooldown_Min"],
                    row_dict["Notes"],
                    row_dict["Intent_ID"],
                    row_dict["Symbol"],
                    row_dict["Decision"],
                    row_dict["Source"],
                ],
                value_input_option="USER_ENTERED",
            )
            return
        except Exception:
            # backoff and try again
            import time as _time

            _time.sleep(delay)
            delay *= 2

    # If all retries failed, persist locally.
    _append_local(row_dict)
