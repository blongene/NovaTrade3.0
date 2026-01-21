#!/usr/bin/env python3
"""wnh_logger.py

Why Nothing Happened (WNH) — shared silence explanation surface.

Design
------
This is a *compiler + mirror* for "inaction" outcomes.

It is intentionally:
  - best-effort (never raises)
  - JSON-first (uses DB_READ_JSON.wnh)
  - DB-first (mirrors to Postgres via db_mirror when enabled)
  - Sheets-friendly (auto-creates the WNH tab + headers)

It does NOT:
  - enqueue commands
  - modify policy decisions

Used by:
  - policy_logger.py (policy decisions)
  - alpha_wnh_mirror.py (alpha proposals/approvals)
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional


DEFAULT_TAB = "Why_Nothing_Happened"


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    wnh = cfg.get("wnh") or {}
    return wnh if isinstance(wnh, dict) else {}


def enabled() -> bool:
    c = _cfg()
    return _truthy(c.get("enabled", 0))


def tab_name() -> str:
    c = _cfg()
    t = str(c.get("tab") or DEFAULT_TAB).strip()
    return t or DEFAULT_TAB


def dedupe_ttl_sec() -> int:
    c = _cfg()
    try:
        v = int(c.get("dedupe_ttl_sec") or 3600)
        return max(60, min(v, 7 * 24 * 3600))
    except Exception:
        return 3600


def sheet_tail_n() -> int:
    c = _cfg()
    try:
        v = int(c.get("sheet_tail_n") or 80)
        return max(10, min(v, 500))
    except Exception:
        return 80


def _now_utc() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def headers() -> List[str]:
    return [
        "Timestamp",
        "Token",
        "Stage",
        "Outcome",
        "Primary_Reason",
        "Secondary_Reasons",
        "Limits_Applied",
        "Autonomy",
        "Decision_ID",
        "Story",
        "Decision_JSON",
        "Intent_JSON",
    ]


_DEDUP_CACHE: Dict[str, float] = {}


def _dedupe_key(token: str, stage: str, outcome: str, primary_reason: str) -> str:
    return "|".join(
        [
            (token or "").strip().upper(),
            (stage or "").strip().upper(),
            (outcome or "").strip().upper(),
            (primary_reason or "").strip(),
        ]
    )


def _should_dedupe(key: str) -> bool:
    ttl = dedupe_ttl_sec()
    now = time.time()
    ts = _DEDUP_CACHE.get(key)
    if ts is not None and (now - ts) < ttl:
        return True
    _DEDUP_CACHE[key] = now
    # prune occasionally
    if len(_DEDUP_CACHE) > 5000:
        cutoff = now - ttl
        for k in list(_DEDUP_CACHE.keys())[:1000]:
            if _DEDUP_CACHE.get(k, 0) < cutoff:
                _DEDUP_CACHE.pop(k, None)
    return False


def _ensure_sheet_headers(tab: str) -> None:
    """Ensure worksheet exists + has headers.

    Prefer utils.ensure_sheet_headers if available; fallback to direct gspread.
    """
    try:
        from utils import ensure_sheet_headers
        ensure_sheet_headers(tab, headers())
        return
    except Exception:
        pass

    # Fallback path (mirrors policy_logger legacy auth style)
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            return

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
        sh = gc.open_by_url(sheet_url)
        try:
            ws = sh.worksheet(tab)
        except Exception:
            ws = sh.add_worksheet(title=tab, rows=4000, cols=max(12, len(headers()) + 2))
            ws.append_row(headers(), value_input_option="USER_ENTERED")
            return

        # If exists, ensure first row matches headers; if empty, write headers.
        try:
            existing = ws.row_values(1)
        except Exception:
            existing = []
        if not existing:
            ws.append_row(headers(), value_input_option="USER_ENTERED")
    except Exception:
        return


def _append_row(tab: str, row: List[Any]) -> None:
    # Prefer cached worksheet helpers
    try:
        from utils import get_ws_cached, ws_append_row
        ws = get_ws_cached(tab, ttl_s=60)
        ws_append_row(ws, row)
        return
    except Exception:
        pass

    # Fallback to legacy open (like policy_logger)
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            return

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
        sh = gc.open_by_url(sheet_url)
        ws = sh.worksheet(tab)
        try:
            ws.append_row(row, value_input_option="USER_ENTERED")
        except TypeError:
            ws.append_row(row)
    except Exception:
        return


def _mirror_db(tab: str, row: List[Any]) -> None:
    try:
        from db_mirror import mirror_append
        mirror_append(tab, [row])
    except Exception:
        return


def _tail_has_key(tab: str, key: str) -> bool:
    """Best-effort sheet tail check to survive restarts.

    If we can read the last N rows, look for a matching dedupe key.
    This is intentionally lightweight; failures return False.
    """
    n = sheet_tail_n()
    try:
        from utils import get_records_cached
        rows = get_records_cached(tab, ttl_s=60) or []
        if not rows:
            return False
        tail = rows[-n:]
        for r in tail:
            tok = str(r.get("Token", "") or "").upper()
            stage = str(r.get("Stage", "") or "")
            outc = str(r.get("Outcome", "") or "")
            prim = str(r.get("Primary_Reason", "") or "")
            if _dedupe_key(tok, stage, outc, prim) == key:
                return True
        return False
    except Exception:
        return False


def emit(
    *,
    token: str,
    stage: str,
    outcome: str,
    primary_reason: str,
    secondary_reasons: Optional[List[str]] = None,
    limits_applied: Optional[List[str]] = None,
    autonomy: str = "",
    decision_id: str = "",
    story: str = "",
    decision_json: Optional[Dict[str, Any]] = None,
    intent_json: Optional[Dict[str, Any]] = None,
    ts: Optional[str] = None,
) -> bool:
    """Emit a WNH row to Sheets + DB mirror (best-effort).

    Returns True if we attempted to write (i.e., not deduped/disabled).
    """
    if not enabled():
        return False

    tab = tab_name()
    token_u = (token or "").strip().upper()
    stage_u = (stage or "").strip().upper()
    outcome_u = (outcome or "").strip().upper()
    prim = (primary_reason or "").strip()
    key = _dedupe_key(token_u, stage_u, outcome_u, prim)

    # De-dupe (in-memory + tail check)
    if _should_dedupe(key) or _tail_has_key(tab, key):
        return False

    _ensure_sheet_headers(tab)

    sec = ";".join([s for s in (secondary_reasons or []) if str(s).strip()])
    lim = ";".join([s for s in (limits_applied or []) if str(s).strip()])

    row = [
        ts or _now_utc(),
        token_u,
        stage_u,
        outcome_u,
        prim,
        sec,
        lim,
        autonomy or "",
        decision_id or "",
        story or "",
        _safe_json(decision_json or {}),
        _safe_json(intent_json or {}),
    ]

    _append_row(tab, row)
    _mirror_db(tab, row)
    return True


def _self_test() -> Dict[str, Any]:
    """Convenience: running `python wnh_logger.py` should create the tab + write one breadcrumb."""
    ok = emit(
        token="SYSTEM",
        stage="WNH",
        outcome="NOOP",
        primary_reason="SELF_TEST",
        secondary_reasons=["If you can read this row, WNH wiring + Sheets access works."],
        story="WNH self-test row (safe).",
        decision_json={"type": "self_test"},
        intent_json={"source": "wnh_logger.__main__"},
    )
    return {"ok": True, "attempted_write": bool(ok), "tab": tab_name()}

def append_row_dict(row: dict) -> dict:
    """
    Append a Why_Nothing_Happened row using header-aware dict mapping.
    Safe for multi-instance use. Returns {ok: bool, reason?: str}.
    """
    try:
        tab = _tab()
        ensure_sheet_headers(tab)

        headers = _headers()
        out = []
        for h in headers:
            out.append(row.get(h, ""))

        _append_row(tab, out)

        # Optional DB mirror (best-effort)
        try:
            _mirror_db(tab, out)
        except Exception:
            pass

        return {"ok": True}
    except Exception as e:
        return {"ok": False, "reason": f"{e.__class__.__name__}:{e}"}

if __name__ == "__main__":
    print(_safe_json(_self_test()))
