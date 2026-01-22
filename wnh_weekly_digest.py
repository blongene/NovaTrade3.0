#!/usr/bin/env python3
"""
wnh_weekly_digest.py

Weekly Why_Nothing_Happened → Council_Insight digest.

Purpose
-------
During observation mode, WNH explains *why* nothing executed. This weekly digest turns that surface
into a single Council_Insight row so you can see trendlines without scrolling.

Design
------
- Observation-safe (no enqueue, no trading)
- Best-effort (never raises)
- DB+Sheet mirror (append to Sheet, then best-effort mirror_append)
- Cross-instance dedupe by decision_id in Council_Insight tail
- JSON-first: reads DB_READ_JSON.wnh.{enabled,tab,weekly_digest.{enabled,tail_n,target_tab}}

Output
------
Appends 1 row/week to Council_Insight with:
- decision_id = wnh_weekly_<YYYY>-W<WW>
- Reason = WNH_WEEKLY_DIGEST
- Story = human-readable summary
- Raw Intent = JSON payload of counts (stages/outcomes/reasons/etc)

If Council_Insight headers differ, we map by header row (source-of-truth) and only fill known fields.
"""
from __future__ import annotations

import os
import json
from datetime import datetime, timezone, timedelta
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
    # Overall WNH master enable
    if not _truthy(_wnh_cfg().get("enabled", 0)):
        return False
    # Weekly digest sub-enable (default ON when wnh.enabled is ON)
    wd = (_wnh_cfg().get("weekly_digest") or {})
    if isinstance(wd, dict) and "enabled" in wd:
        return _truthy(wd.get("enabled"))
    return True


def _wnh_tab() -> str:
    return str(_wnh_cfg().get("tab") or "Why_Nothing_Happened").strip() or "Why_Nothing_Happened"


def _target_tab() -> str:
    wd = (_wnh_cfg().get("weekly_digest") or {})
    if isinstance(wd, dict):
        t = str(wd.get("target_tab") or "Council_Insight").strip()
        if t:
            return t
    return "Council_Insight"


def _tail_n() -> int:
    wd = (_wnh_cfg().get("weekly_digest") or {})
    try:
        n = int((wd or {}).get("tail_n") or 250)
        return max(60, min(n, 2000))
    except Exception:
        return 250


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_ci_ts(dt: datetime) -> str:
    # Council_Insight historically uses M/D/YYYY H:MM:SS
    return f"{dt.month}/{dt.day}/{dt.year} {dt.hour}:{dt.minute:02d}:{dt.second:02d}"


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        return "{}"


def _parse_ts_any(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("T", " ")
    # common: "YYYY-MM-DD HH:MM:SS" (UTC)
    try:
        if "." in s:
            s = s.split(".", 1)[0]
        if len(s) == 10 and s[4] == "-":
            dt = datetime.strptime(s, "%Y-%m-%d")
        else:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        # as a last resort, accept M/D/YYYY H:MM:SS
        try:
            dt = datetime.strptime(s, "%m/%d/%Y %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            return None


def _read_wnh_rows() -> List[Dict[str, Any]]:
    tab = _wnh_tab()

    # Prefer DB-first adapter if available (DB+Sheet mirror architecture)
    try:
        from db_read_adapter import get_records_prefer_db  # type: ignore
        from utils import get_records_cached  # type: ignore

        rows = get_records_prefer_db(
            tab,
            f"sheet_mirror:{tab}",
            sheets_fallback_fn=lambda *args, **kwargs: get_records_cached(tab),
        )
        return rows if isinstance(rows, list) else []
    except Exception:
        pass

    # Fallback to Sheets cached reader
    try:
        from utils import get_records_cached  # type: ignore
        rows = get_records_cached(tab)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _count_by(rows: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        v = (r.get(key) or "") if isinstance(r, dict) else ""
        s = str(v).strip() or "(blank)"
        out[s] = out.get(s, 0) + 1
    return out


def _top_k(d: Dict[str, int], k: int = 6) -> List[Tuple[str, int]]:
    return sorted(d.items(), key=lambda x: (-x[1], x[0]))[:k]


def _filter_window(rows: List[Dict[str, Any]], start_ts: float, end_ts: float) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        ts = _parse_ts_any((r or {}).get("Timestamp"))
        if ts is None:
            continue
        if start_ts <= ts <= end_ts:
            out.append(r)
    return out


def _iso_week_id(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _get_ws(tab: str):
    from utils import get_ws_cached  # type: ignore
    return get_ws_cached(tab, ttl_s=30)


def _get_all_values(ws) -> List[List[str]]:
    try:
        return ws.get_all_values() or []
    except Exception:
        return []


def _ensure_headers_if_empty(tab: str, ws) -> List[str]:
    vals = _get_all_values(ws)
    if vals and vals[0]:
        return vals[0]

    # If empty, create a minimal Council_Insight header (your sheet usually already exists)
    header = [
        "Timestamp",
        "decision_id",
        "Autonomy",
        "OK",
        "Reason",
        "Story",
        "Ash's Lens",
        "Soul",
        "Nova",
        "Orion",
        "Ash",
        "Lumen",
        "Vigil",
        "Raw Intent",
        "Patched",
        "Flags",
        "Exec Timestamp",
        "Exec Status",
        "Exec Cmd_ID",
        "Exec Notional_USD",
        "Exec Quote",
        "Outcome Tag",
        "Mark Price_USD",
        "PnL_USD_Current",
        "PnL_Tag_Current",
    ]
    try:
        ws.append_row(header, value_input_option="USER_ENTERED")
    except Exception:
        ws.append_row(header)
    return header


def _tail_has_decision_id(vals: List[List[str]], decision_id: str, tail_n: int) -> bool:
    if not vals:
        return False
    header = vals[0] or []
    try:
        i = header.index("decision_id")
    except Exception:
        return False
    tail = vals[-tail_n:] if len(vals) > tail_n else vals[1:]
    for r in tail:
        if len(r) > i and (r[i] or "").strip() == decision_id:
            return True
    return False


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


def run_wnh_weekly_digest(force: bool = False, window_days: int = 7) -> Dict[str, Any]:
    if not _enabled():
        return {"ok": False, "skipped": True, "reason": "wnh.disabled"}

    now = _utc_now_dt()
    week_id = _iso_week_id(now)
    decision_id = f"wnh_weekly_{week_id}"

    start = now - timedelta(days=max(1, int(window_days)))
    start_ts = start.timestamp()
    end_ts = now.timestamp()

    # Compute weekly stats
    rows_all = _read_wnh_rows()
    rows = _filter_window(rows_all, start_ts, end_ts)

    stage_counts = _count_by(rows, "Stage")
    outcome_counts = _count_by(rows, "Outcome")
    reason_counts = _count_by(rows, "Primary_Reason")

    # Secondary reasons are comma-separated; explode for top blockers
    sec_counts: Dict[str, int] = {}
    for r in rows:
        s = str((r or {}).get("Secondary_Reasons") or "").strip()
        if not s:
            continue
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for p in parts:
            sec_counts[p] = sec_counts.get(p, 0) + 1

    top_primary = _top_k(reason_counts, k=6)
    top_secondary = _top_k(sec_counts, k=8)

    top_primary_str = ", ".join([f"{k}={v}" for k, v in top_primary]) if top_primary else "none"
    top_secondary_str = ", ".join([f"{k}={v}" for k, v in top_secondary]) if top_secondary else "none"

    story = (
        f"WNH Weekly Digest (UTC {start.strftime('%Y-%m-%d')}→{now.strftime('%Y-%m-%d')}): "
        f"rows={len(rows)} | stages={_safe_json(stage_counts)} | outcomes={_safe_json(outcome_counts)} | "
        f"Primary={top_primary_str} | Secondary={top_secondary_str}"
    )

    payload = {
        "week_id": week_id,
        "window_utc": {
            "start": start.isoformat(),
            "end": now.isoformat(),
            "days": int(window_days),
        },
        "rows": int(len(rows)),
        "stage_counts": stage_counts,
        "outcome_counts": outcome_counts,
        "top_primary_reasons": top_primary,
        "top_secondary_reasons": top_secondary,
    }

    target_tab = _target_tab()

    # Dedupe (read target sheet tail)
    try:
        ws = _get_ws(target_tab)
        vals = _get_all_values(ws)
        header = _ensure_headers_if_empty(target_tab, ws)
        vals = _get_all_values(ws)  # refresh after header write
    except Exception as e:
        return {"ok": False, "skipped": True, "reason": f"ws_open_failed:{e.__class__.__name__}"}

    if not force:
        try:
            if _tail_has_decision_id(vals, decision_id, _tail_n()):
                return {"ok": True, "rows": 0, "deduped": True, "decision_id": decision_id}
        except Exception:
            pass

    # Build row in target header order (source-of-truth)
    # Fill known fields; unknown remain blank.
    row_map: Dict[str, Any] = {
        "Timestamp": _fmt_ci_ts(now),
        "decision_id": decision_id,
        "Autonomy": "wnh_weekly_digest",
        "OK": "TRUE",
        "Reason": "WNH_WEEKLY_DIGEST",
        "Story": story,
        "Ash's Lens": "clean",
        "Soul": "0",
        "Nova": "1",
        "Orion": "0",
        "Ash": "0",
        "Lumen": "0",
        "Vigil": "0",
        "Raw Intent": _safe_json(payload),
        "Flags": _safe_json(["wnh_weekly", week_id]),
        "Outcome Tag": "WNH_WEEKLY",
    }

    header = vals[0] if vals else header  # type: ignore
    out_row: List[Any] = []
    for h in header:
        out_row.append(row_map.get(h, ""))

    try:
        _append_row(target_tab, out_row)
    except Exception as e:
        return {"ok": False, "rows": 0, "reason": f"append_failed:{e.__class__.__name__}"}

    # Optional DB mirror (best-effort)
    try:
        from db_mirror import mirror_append  # type: ignore
        mirror_append(target_tab, [out_row])
    except Exception:
        pass

    return {"ok": True, "rows": 1, "decision_id": decision_id, "week_id": week_id}


if __name__ == "__main__":
    print(_safe_json(run_wnh_weekly_digest()))
