# wnh_daily_summary.py
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


def _wnh_enabled() -> bool:
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


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        return "{}"


def _sig(day: str) -> str:
    # stable, cross-instance signature
    return hashlib.sha256(f"WNH|DAILY_SUMMARY|{day}".encode("utf-8")).hexdigest()[:16]


def _get_ws(tab: str):
    # Prefer your cached ws helper if present
    from utils import get_ws_cached  # type: ignore
    return get_ws_cached(tab, ttl_s=30)


def _read_all_values(ws) -> List[List[str]]:
    try:
        return ws.get_all_values() or []
    except Exception:
        return []


def _header_index(header: List[str], name: str) -> int:
    try:
        return header.index(name)
    except ValueError:
        return -1


def _tail_signatures(ws, tail_n: int) -> set:
    vals = _read_all_values(ws)
    if not vals:
        return set()
    header = vals[0]
    idx = _header_index(header, "Signature")
    if idx < 0:
        return set()
    tail = vals[-tail_n:] if len(vals) > tail_n else vals[1:]
    out = set()
    for r in tail:
        if len(r) > idx:
            s = (r[idx] or "").strip()
            if s:
                out.add(s)
    return out


def _rows_for_day(vals: List[List[str]], day: str) -> Tuple[List[str], List[List[str]]]:
    if not vals:
        return [], []
    header = vals[0]
    ts_i = _header_index(header, "Timestamp")
    if ts_i < 0:
        return header, []

    keep = []
    for r in vals[1:]:
        if len(r) <= ts_i:
            continue
        ts = (r[ts_i] or "").strip()
        if ts.startswith(day):
            keep.append(r)
    return header, keep


def _count_by(header: List[str], rows: List[List[str]], colname: str) -> Dict[str, int]:
    idx = _header_index(header, colname)
    if idx < 0:
        return {}
    out: Dict[str, int] = {}
    for r in rows:
        v = (r[idx] if len(r) > idx else "") or ""
        v = v.strip() or "(blank)"
        out[v] = out.get(v, 0) + 1
    return out


def _top_k(d: Dict[str, int], k: int = 5) -> List[Tuple[str, int]]:
    return sorted(d.items(), key=lambda x: (-x[1], x[0]))[:k]


def _append_row_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    # Uses the helper you already added to wnh_logger.py
    import wnh_logger
    if hasattr(wnh_logger, "append_row_dict"):
        return wnh_logger.append_row_dict(row)  # type: ignore
    return {"ok": False, "reason": "wnh_logger.append_row_dict_missing"}


def run_wnh_daily_summary(force: bool = False) -> Dict[str, Any]:
    if not _wnh_enabled():
        return {"ok": False, "skipped": True, "reason": "wnh.disabled"}

    day = _utc_day()
    signature = _sig(day)
    tab = _tab()

    try:
        ws = _get_ws(tab)
    except Exception as e:
        return {"ok": False, "skipped": True, "reason": f"ws_open_failed:{e.__class__.__name__}"}

    # Cross-instance dedupe by checking sheet tail
    try:
        if not force:
            if signature in _tail_signatures(ws, _tail_n()):
                return {"ok": True, "rows": 0, "deduped": True}
    except Exception:
        pass

    vals = _read_all_values(ws)
    header, rows = _rows_for_day(vals, day)

    stage_counts = _count_by(header, rows, "Stage")
    outcome_counts = _count_by(header, rows, "Outcome")
    reason_counts = _count_by(header, rows, "Primary_Reason")

    top_reasons = _top_k(reason_counts, k=5)
    top_reasons_str = ", ".join([f"{k}={v}" for k, v in top_reasons]) if top_reasons else "none"

    story = (
        f"WNH Daily Summary (UTC {day}): "
        f"rows={len(rows)} | "
        f"Stages={_safe_json(stage_counts)} | "
        f"Outcomes={_safe_json(outcome_counts)} | "
        f"TopReasons={top_reasons_str}"
    )

    row = {
        "Timestamp": _utc_now(),
        "Token": "SYSTEM",
        "Stage": "WNH",
        "Outcome": "NOOP",
        "Primary_Reason": "DAILY_SUMMARY",
        "Secondary_Reasons": top_reasons_str,
        "Limits_Applied": "",
        "Autonomy": "",
        "Decision_ID": day,
        "Story": story,
        "Decision_JSON": _safe_json(
            {
                "utc_day": day,
                "rows": len(rows),
                "stage_counts": stage_counts,
                "outcome_counts": outcome_counts,
                "top_reasons": top_reasons,
            }
        ),
        "Intent_JSON": _safe_json({"source": "wnh_daily_summary"}),
        "Signature": signature,
    }

    out = _append_row_dict(row)
    return {"ok": bool(out.get("ok")), "rows": 1 if out.get("ok") else 0, "append": out}


if __name__ == "__main__":
    print(_safe_json(run_wnh_daily_summary()))
