#!/usr/bin/env python3
"""
wnh_weekly_digest.py

Weekly Why_Nothing_Happened → Council_Insight digest (with Token Leaderboard).

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
- JSON-first: reads DB_READ_JSON.wnh.{enabled,tab,weekly_digest{...}}

Config (DB_READ_JSON.wnh.weekly_digest)
--------------------------------------
{
  "enabled": 1,                  # default True when wnh.enabled is True
  "target_tab": "Council_Insight",
  "tail_n": 250,                 # dedupe scan tail rows
  "window_days": 7,              # rolling window length
  "drop_tokens": ["SYSTEM"],     # tokens to ignore in digest stats
  "drop_primary": ["DAILY_SUMMARY", "SELF_TEST", "SELF_TEST_POLICY_DENY (safe)"],
  "primary_map": {"APPROVED_DRYRUN": "APPROVED_BUT_GATED"},
  "top_tokens_k": 8,             # token leaderboard size
  "top_primary_k": 6,
  "top_secondary_k": 8
}

Output
------
Appends 1 row/week to Council_Insight with:
- decision_id = wnh_weekly_<YYYY>-W<WW>
- Reason = WNH_WEEKLY_DIGEST
- Story = polished human-readable summary (includes token leaderboard)
- Raw Intent = JSON payload of counts + filters applied

If Council_Insight headers differ, we map by header row (source-of-truth) and only fill known fields.
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional


# -----------------------
# tiny helpers
# -----------------------

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
        return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        return "{}"


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        o = json.loads(raw)
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}


def _wnh_cfg() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    w = cfg.get("wnh") or {}
    return w if isinstance(w, dict) else {}


def _weekly_cfg() -> Dict[str, Any]:
    wd = (_wnh_cfg().get("weekly_digest") or {})
    return wd if isinstance(wd, dict) else {}


def _enabled() -> bool:
    # master enable
    if not _truthy(_wnh_cfg().get("enabled", 0)):
        return False
    # weekly sub-enable (default ON when master ON)
    wd = _weekly_cfg()
    if "enabled" in wd:
        return _truthy(wd.get("enabled"))
    return True


def _wnh_tab() -> str:
    t = str(_wnh_cfg().get("tab") or "Why_Nothing_Happened").strip()
    return t or "Why_Nothing_Happened"


def _target_tab() -> str:
    t = str(_weekly_cfg().get("target_tab") or "Council_Insight").strip()
    return t or "Council_Insight"


def _tail_n() -> int:
    try:
        n = int(_weekly_cfg().get("tail_n") or 250)
        return max(60, min(n, 2000))
    except Exception:
        return 250


def _window_days(default_days: int = 7) -> int:
    try:
        n = int(_weekly_cfg().get("window_days") or default_days)
        return max(1, min(n, 30))
    except Exception:
        return default_days


def _top_primary_k() -> int:
    try:
        n = int(_weekly_cfg().get("top_primary_k") or 6)
        return max(3, min(n, 15))
    except Exception:
        return 6


def _top_secondary_k() -> int:
    try:
        n = int(_weekly_cfg().get("top_secondary_k") or 8)
        return max(3, min(n, 20))
    except Exception:
        return 8


def _top_tokens_k() -> int:
    try:
        n = int(_weekly_cfg().get("top_tokens_k") or 8)
        return max(3, min(n, 25))
    except Exception:
        return 8


def _drop_tokens() -> List[str]:
    v = _weekly_cfg().get("drop_tokens")
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    # default: ignore SYSTEM noise
    return ["SYSTEM"]


def _drop_primary() -> List[str]:
    v = _weekly_cfg().get("drop_primary")
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    # default: ignore self-tests and daily summaries in weekly view
    return ["DAILY_SUMMARY", "SELF_TEST", "SELF_TEST_POLICY_DENY (safe)"]


def _primary_map() -> Dict[str, str]:
    v = _weekly_cfg().get("primary_map")
    if isinstance(v, dict):
        out: Dict[str, str] = {}
        for k, val in v.items():
            ks = str(k).strip()
            vs = str(val).strip()
            if ks and vs:
                out[ks] = vs
        return out
    # default: treat APPROVED_DRYRUN as the same “lane” as gated approvals
    return {"APPROVED_DRYRUN": "APPROVED_BUT_GATED"}


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_ci_ts(dt: datetime) -> str:
    # Council_Insight convention: M/D/YYYY H:MM:SS
    return f"{dt.month}/{dt.day}/{dt.year} {dt.hour}:{dt.minute:02d}:{dt.second:02d}"


def _parse_ts_any(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("T", " ")
    try:
        if "." in s:
            s = s.split(".", 1)[0]
        if len(s) == 10 and s[4] == "-":
            dt = datetime.strptime(s, "%Y-%m-%d")
        else:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        try:
            dt = datetime.strptime(s, "%m/%d/%Y %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            return None


def _iso_week_id(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


# -----------------------
# reads
# -----------------------

def _read_wnh_rows() -> List[Dict[str, Any]]:
    tab = _wnh_tab()

    # Prefer DB-first adapter if available
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

    # Sheets fallback
    try:
        from utils import get_records_cached  # type: ignore
        rows = get_records_cached(tab)
        return rows if isinstance(rows, list) else []
    except Exception:
        return []


def _filter_window(rows: List[Dict[str, Any]], start_ts: float, end_ts: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        ts = _parse_ts_any(r.get("Timestamp"))
        if ts is None:
            continue
        if start_ts <= ts <= end_ts:
            out.append(r)
    return out


# -----------------------
# counting
# -----------------------

def _count_by(rows: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        v = r.get(key) if isinstance(r, dict) else ""
        s = str(v or "").strip() or "(blank)"
        out[s] = out.get(s, 0) + 1
    return out


def _top_k(d: Dict[str, int], k: int) -> List[Tuple[str, int]]:
    return sorted(d.items(), key=lambda x: (-x[1], x[0]))[:k]


def _explode_secondary(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    sec_counts: Dict[str, int] = {}
    for r in rows:
        s = str((r or {}).get("Secondary_Reasons") or "").strip()
        if not s:
            continue
        parts = [p.strip() for p in s.split(",") if p.strip()]
        for p in parts:
            sec_counts[p] = sec_counts.get(p, 0) + 1
    return sec_counts


def _apply_filters(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    drop_tokens = set(x.upper() for x in _drop_tokens())
    drop_primary = set(x.upper() for x in _drop_primary())
    primary_map = _primary_map()

    kept: List[Dict[str, Any]] = []
    for r in rows:
        token = str((r or {}).get("Token") or "").strip()
        primary = str((r or {}).get("Primary_Reason") or "").strip()

        if token and token.upper() in drop_tokens:
            continue
        if primary and primary.upper() in drop_primary:
            continue

        # map primary reason (e.g., APPROVED_DRYRUN -> APPROVED_BUT_GATED)
        if primary in primary_map:
            r = dict(r)
            r["Primary_Reason"] = primary_map[primary]

        kept.append(r)

    meta = {
        "drop_tokens": list(_drop_tokens()),
        "drop_primary": list(_drop_primary()),
        "primary_map": dict(_primary_map()),
    }
    return kept, meta


# -----------------------
# sheets append (Council_Insight)
# -----------------------

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

    # Create a minimal Council_Insight header if the tab is empty
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
    # Prefer shared helper if present (rate-limit safe)
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


# -----------------------
# main
# -----------------------

def run_wnh_weekly_digest(force: bool = False, window_days: Optional[int] = None) -> Dict[str, Any]:
    """
    Returns:
      { ok: bool, rows: int, decision_id: str, week_id: str, tab: str, deduped?: bool, reason?: str }
    """
    if not _enabled():
        return {"ok": False, "skipped": True, "reason": "wnh.disabled"}

    now = _utc_now_dt()
    week_id = _iso_week_id(now)
    decision_id = f"wnh_weekly_{week_id}"

    days = int(window_days) if window_days is not None else _window_days(7)
    start = now - timedelta(days=max(1, days))
    start_ts = start.timestamp()
    end_ts = now.timestamp()

    # Gather + filter
    rows_all = _read_wnh_rows()
    rows_window = _filter_window(rows_all, start_ts, end_ts)
    rows, filters_meta = _apply_filters(rows_window)

    # Counts
    stage_counts = _count_by(rows, "Stage")
    outcome_counts = _count_by(rows, "Outcome")
    primary_counts = _count_by(rows, "Primary_Reason")
    token_counts = _count_by(rows, "Token")
    sec_counts = _explode_secondary(rows)

    top_primary = _top_k(primary_counts, k=_top_primary_k())
    top_secondary = _top_k(sec_counts, k=_top_secondary_k())
    top_tokens = _top_k(token_counts, k=_top_tokens_k())

    def _fmt_pairs(pairs: List[Tuple[str, int]]) -> str:
        return ", ".join([f"{k}={v}" for k, v in pairs]) if pairs else "none"

    # Polished story
    story = (
        f"WNH Weekly Digest (UTC {start.strftime('%Y-%m-%d')}→{now.strftime('%Y-%m-%d')}): "
        f"rows={len(rows)} | stages={_safe_json(stage_counts)} | outcomes={_safe_json(outcome_counts)} | "
        f"Primary={_fmt_pairs(top_primary)} | Secondary={_fmt_pairs(top_secondary)} | "
        f"Tokens={_fmt_pairs(top_tokens)}"
    )

    payload = {
        "week_id": week_id,
        "window_utc": {"start": start.isoformat(), "end": now.isoformat(), "days": int(days)},
        "rows": int(len(rows)),
        "stage_counts": stage_counts,
        "outcome_counts": outcome_counts,
        "top_primary_reasons": top_primary,
        "top_secondary_reasons": top_secondary,
        "top_tokens": top_tokens,
        "filters": filters_meta,
    }

    target_tab = _target_tab()

    # Dedupe against Council_Insight tail
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
                return {"ok": True, "rows": 0, "deduped": True, "decision_id": decision_id, "week_id": week_id, "tab": target_tab}
        except Exception:
            pass

    # Row mapping by header (source-of-truth)
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
    out_row: List[Any] = [row_map.get(h, "") for h in header]

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

    return {"ok": True, "rows": 1, "decision_id": decision_id, "week_id": week_id, "tab": target_tab}


if __name__ == "__main__":
    print(_safe_json(run_wnh_weekly_digest()))
