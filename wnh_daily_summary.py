#!/usr/bin/env python3
"""
wnh_daily_summary.py

Writes a single roll-up row per UTC day into the shared Why_Nothing_Happened surface.
- Observation-safe (no enqueue, no trading)
- Best-effort (never raises)
- Cross-instance dedupe via Signature in sheet tail
- JSON-first: reads DB_READ_JSON.wnh.{enabled,tab,sheet_tail_n}

Row format matches the shared WNH headers (adds Signature column).
"""
from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _wnh_cfg() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    w = cfg.get("wnh") or {}
    return w if isinstance(w, dict) else {}


def _enabled() -> bool:
    return _truthy(_wnh_cfg().get("enabled", 0))


def _tab() -> str:
    return str(_wnh_cfg().get("tab") or "Why_Nothing_Happened").strip() or "Why_Nothing_Happened"


def _tail_n() -> int:
    try:
        n = int(_wnh_cfg().get("sheet_tail_n") or 200)
        return max(40, min(n, 2000))
    except Exception:
        return 200


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _sig(day: str) -> str:
    return hashlib.sha256(f"WNH|DAILY_SUMMARY|{day}".encode("utf-8")).hexdigest()[:16]


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        return "{}"


def _headers() -> List[str]:
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
        "Signature",
    ]


def _get_ws(tab: str):
    from utils import get_ws_cached  # type: ignore
    return get_ws_cached(tab, ttl_s=30)


def _get_all_values(ws) -> List[List[str]]:
    try:
        return ws.get_all_values() or []
    except Exception:
        return []


def _tail_signatures(ws, tail_n: int) -> set:
    vals = _get_all_values(ws)
    if not vals:
        return set()
    header = vals[0] if vals else []
    try:
        sig_i = header.index("Signature")
    except Exception:
        return set()
    tail = vals[-tail_n:] if len(vals) > tail_n else vals[1:]
    out = set()
    for r in tail:
        if len(r) > sig_i:
            s = (r[sig_i] or "").strip()
            if s:
                out.add(s)
    return out


def _rows_for_day(vals: List[List[str]], day: str) -> Tuple[List[str], List[List[str]]]:
    if not vals:
        return [], []
    header = vals[0]
    try:
        ts_i = header.index("Timestamp")
    except Exception:
        return header, []
    rows = []
    for r in vals[1:]:
        if len(r) <= ts_i:
            continue
        ts = (r[ts_i] or "").strip()
        if ts.startswith(day):
            rows.append(r)
    return header, rows


def _count_by(header: List[str], rows: List[List[str]], col: str) -> Dict[str, int]:
    try:
        i = header.index(col)
    except Exception:
        return {}
    out: Dict[str, int] = {}
    for r in rows:
        v = (r[i] if len(r) > i else "") or ""
        v = v.strip() or "(blank)"
        out[v] = out.get(v, 0) + 1
    return out


def _top_k(d: Dict[str, int], k: int = 5) -> List[Tuple[str, int]]:
    return sorted(d.items(), key=lambda x: (-x[1], x[0]))[:k]


def _ensure_headers(tab: str) -> None:
    try:
        from utils import ensure_sheet_headers  # type: ignore
        ensure_sheet_headers(tab, _headers())
    except Exception:
        # best-effort fallback: write header if tab empty
        ws = _get_ws(tab)
        vals = _get_all_values(ws)
        if not vals:
            try:
                ws.append_row(_headers(), value_input_option="USER_ENTERED")
            except Exception:
                ws.append_row(_headers())


def _append_row(tab: str, row: List[Any]) -> None:
    # Prefer shared append helper if present (rate-limit safe)
    try:
        from utils import get_ws_cached, ws_append_row  # type: ignore
        ws = get_ws_cached(tab, ttl_s=30)
        ws_append_row(ws, row)
        return
    except Exception:
        pass
    ws = _get_ws(tab)
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception:
        ws.append_row(row)


def run_wnh_daily_summary(force: bool = False) -> Dict[str, Any]:
    if not _enabled():
        return {"ok": False, "skipped": True, "reason": "wnh.disabled"}

    tab = _tab()
    day = _utc_day()
    signature = _sig(day)

    try:
        _ensure_headers(tab)
        ws = _get_ws(tab)
    except Exception as e:
        return {"ok": False, "skipped": True, "reason": f"ws_open_failed:{e.__class__.__name__}"}

    if not force:
        try:
            if signature in _tail_signatures(ws, _tail_n()):
                return {"ok": True, "rows": 0, "deduped": True}
        except Exception:
            pass

    vals = _get_all_values(ws)
    header, rows = _rows_for_day(vals, day)

    stage_counts = _count_by(header, rows, "Stage")
    outcome_counts = _count_by(header, rows, "Outcome")
    reason_counts = _count_by(header, rows, "Primary_Reason")

    top_reasons = _top_k(reason_counts, k=5)
    top_str = ", ".join([f"{k}={v}" for k, v in top_reasons]) if top_reasons else "none"

    story = (
        f"WNH Daily Summary (UTC {day}): rows={len(rows)} | "
        f"Stages={_safe_json(stage_counts)} | Outcomes={_safe_json(outcome_counts)} | "
        f"TopReasons={top_str}"
    )

    row = [
        _utc_now(),
        "SYSTEM",
        "WNH",
        "NOOP",
        "DAILY_SUMMARY",
        top_str if top_str != "none" else "none",
        "",
        "",
        day,
        story,
        _safe_json({
            "utc_day": day,
            "rows": len(rows),
            "stage_counts": stage_counts,
            "outcome_counts": outcome_counts,
            "top_reasons": top_reasons,
        }),
        _safe_json({"source": "wnh_daily_summary"}),
        signature,
    ]

    try:
        _append_row(tab, row)
    except Exception as e:
        return {"ok": False, "rows": 0, "reason": f"append_failed:{e.__class__.__name__}"}

    # Optional DB mirror (best-effort)
    try:
        from db_mirror import mirror_append  # type: ignore
        mirror_append(tab, [row])
    except Exception:
        pass

    return {"ok": True, "rows": 1, "signature": signature}


if __name__ == "__main__":
    print(_safe_json(run_wnh_daily_summary()))
