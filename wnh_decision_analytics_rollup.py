# wnh_decision_analytics_rollup.py
# Phase 26+ — WNH → Decision_Analytics rollup (presentation + DB mirror)
#
# Contract:
# - Appends ONE row per UTC day into Decision_Analytics
# - Idempotent: uses decision_id = "wnh_daily_YYYY-MM-DD"
# - Does not require changing Decision_Analytics headers
# - Fail-open: never breaks scheduler

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


DECISION_ANALYTICS_TAB = os.getenv("DECISION_ANALYTICS_WS", "Decision_Analytics")


def _now_ts_str() -> str:
    # Match your sheet format style; keep simple.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _open_ws(tab: str):
    # Prefer cached helper
    try:
        from utils import get_ws_cached  # type: ignore
        return get_ws_cached(tab, ttl_s=30)
    except Exception:
        # fallback: raw gspread (rare; usually utils exists)
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials

        sheet_url = os.getenv("SHEET_URL")
        if not sheet_url:
            raise RuntimeError("SHEET_URL not set")

        svc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not svc:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set")

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(sheet_url)

        try:
            return sh.worksheet(tab)
        except Exception:
            return sh.add_worksheet(title=tab, rows=4000, cols=30)


def _get_header(ws) -> List[str]:
    try:
        h = ws.row_values(1)
        return [x.strip() for x in h if str(x).strip()]
    except Exception:
        return []


def _tail_records(ws, n: int = 200) -> List[Dict[str, Any]]:
    # We avoid get_all_records on huge tabs; tail read by values.
    try:
        vals = ws.get_all_values()
        if not vals:
            return []
        header = vals[0]
        body = vals[1:]
        tail = body[-n:] if len(body) > n else body
        out: List[Dict[str, Any]] = []
        for row in tail:
            d = {}
            for i, k in enumerate(header):
                if not k:
                    continue
                d[k] = row[i] if i < len(row) else ""
            out.append(d)
        return out
    except Exception:
        try:
            return ws.get_all_records()  # fallback
        except Exception:
            return []


def _append_row_dict(tab: str, row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Header-driven append into Decision_Analytics (or any sheet tab).
    Mirrors to DB via db_mirror if allowed.
    """
    ws = _open_ws(tab)
    header = _get_header(ws)
    if not header:
        return {"ok": False, "reason": "missing_header_row"}

    out = [row_dict.get(h, "") for h in header]

    try:
        ws.append_row(out, value_input_option="USER_ENTERED")
    except Exception:
        ws.append_row(out)

    # Optional DB mirror
    try:
        from db_mirror import mirror_append  # type: ignore
        mirror_append(tab, [out])
    except Exception:
        pass

    return {"ok": True}


def emit_wnh_daily_rollup(utc_day: str, summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    summary shape (from wnh_daily_summary):
      {
        "utc_day": "YYYY-MM-DD",
        "rows": int,
        "stage_counts": {...},
        "outcome_counts": {...},
        "top_reasons": [[reason, count], ...]
      }
    """
    ws = _open_ws(DECISION_ANALYTICS_TAB)

    decision_id = f"wnh_daily_{utc_day}"

    # Idempotency: if already present, skip
    tail = _tail_records(ws, n=250)
    for r in tail:
        if str(r.get("decision_id") or "").strip() == decision_id:
            return {"ok": True, "skipped": True, "reason": "already_emitted", "decision_id": decision_id}

    rows = int(summary.get("rows") or 0)
    stage_counts = summary.get("stage_counts") or {}
    outcome_counts = summary.get("outcome_counts") or {}
    top = summary.get("top_reasons") or []

    top_str = ", ".join([f"{a}={b}" for a, b in top[:6]]) if top else "none"
    stage_str = ", ".join([f"{k}={v}" for k, v in stage_counts.items()]) if stage_counts else "none"
    outcome_str = ", ".join([f"{k}={v}" for k, v in outcome_counts.items()]) if outcome_counts else "none"

    # Fit into your Decision_Analytics columns:
    # Timestamp, decision_id, Autonomy, OK, Ash's Lens, Disagreement_Index, Majority_Voice, Exec Status, Outcome Tag, Soul...
    row = {
        "Timestamp": _now_ts_str(),
        "decision_id": decision_id,
        "Autonomy": "wnh_rollup",
        "OK": "TRUE",
        "Ash's Lens": f"WNH Daily (UTC {utc_day}): rows={rows} | stages={stage_str} | outcomes={outcome_str} | top={top_str}",
        "Outcome Tag": "WNH_DAILY",
        "Exec Status": "",
        "Disagreement_Index": "",
        "Majority_Voice": "",
        "Soul": "",
        "Nova": "",
        "Orion": "",
        "Ash": "",
        "Lumen": "",
        "Vigil": "",
    }

    res = _append_row_dict(DECISION_ANALYTICS_TAB, row)
    if not res.get("ok"):
        return res

    return {"ok": True, "decision_id": decision_id, "tab": DECISION_ANALYTICS_TAB}
