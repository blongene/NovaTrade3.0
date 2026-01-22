# wnh_weekly_digest.py
# Weekly WNH → Council_Insight digest (presentation-only)
# - Rolls up last N days (default 7) from Why_Nothing_Happened
# - Writes 1 row per ISO week: decision_id=wnh_weekly_YYYY-Www
# - Idempotent: skips if decision_id already exists (unless force=True)
# - Polished: filters test/wiring noise, normalizes APPROVED_DRYRUN -> APPROVED_BUT_GATED

from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_WNH_TAB = "Why_Nothing_Happened"
DEFAULT_TARGET_TAB = "Council_Insight"


# ---------- config helpers ----------

def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def _cfg() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    wnh = cfg.get("wnh") or {}
    if isinstance(wnh, dict):
        weekly = wnh.get("weekly_digest") or {}
        if isinstance(weekly, dict):
            return weekly
    return {}

def _wnh_tab() -> str:
    cfg = _load_db_read_json()
    wnh = cfg.get("wnh") or {}
    if isinstance(wnh, dict):
        t = str(wnh.get("tab") or "").strip()
        if t:
            return t
    return DEFAULT_WNH_TAB

def _target_tab() -> str:
    c = _cfg()
    t = str(c.get("target_tab") or "").strip()
    return t or DEFAULT_TARGET_TAB

def _enabled() -> bool:
    c = _cfg()
    # default ON if file exists (safe: idempotent + presentation-only)
    if "enabled" in c:
        return _truthy(c.get("enabled"))
    return True

def _tail_n() -> int:
    c = _cfg()
    try:
        n = int(c.get("tail_n") or 300)
        return max(50, min(n, 2000))
    except Exception:
        return 300

def _window_days() -> int:
    c = _cfg()
    try:
        d = int(c.get("window_days") or 7)
        return max(3, min(d, 21))
    except Exception:
        return 7

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _now_ts_str_localish() -> str:
    # your sheet shows "1/22/2026 2:31:01" style; we mimic month/day/year no leading zeros
    dt = _now_utc().astimezone(timezone.utc)
    return dt.strftime("%-m/%-d/%Y %-H:%M:%S") if os.name != "nt" else dt.strftime("%m/%d/%Y %H:%M:%S")


# ---------- sheets helpers ----------

def _open_ws(tab: str):
    try:
        from utils import get_ws_cached  # type: ignore
        return get_ws_cached(tab, ttl_s=30)
    except Exception:
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

def _get_all_values(ws) -> List[List[str]]:
    try:
        return ws.get_all_values()
    except Exception:
        return []

def _header(ws) -> List[str]:
    vals = _get_all_values(ws)
    if not vals:
        return []
    return [str(x).strip() for x in vals[0]]

def _rows_as_dicts(ws, tail_n: int) -> List[Dict[str, Any]]:
    vals = _get_all_values(ws)
    if not vals or len(vals) < 2:
        return []
    hdr = vals[0]
    body = vals[1:]
    tail = body[-tail_n:] if len(body) > tail_n else body
    out: List[Dict[str, Any]] = []
    for r in tail:
        d: Dict[str, Any] = {}
        for i, k in enumerate(hdr):
            if not k:
                continue
            d[k] = r[i] if i < len(r) else ""
        out.append(d)
    return out

def _append_row_dict(tab: str, row_dict: Dict[str, Any]) -> Dict[str, Any]:
    ws = _open_ws(tab)
    hdr = _header(ws)
    if not hdr:
        return {"ok": False, "reason": "missing_header_row"}

    out = [row_dict.get(h, "") for h in hdr]

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


# ---------- parsing + normalization ----------

def _parse_ts_any(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None

    # WNH timestamps appear as "YYYY-MM-DD HH:MM:SS" in your sheet.
    s_norm = s.replace("T", " ")
    try:
        if "." in s_norm:
            s_norm = s_norm.split(".", 1)[0]
        dt = datetime.strptime(s_norm, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # Sometimes Sheets render "1/22/2026 2:31:01"
    try:
        dt = datetime.strptime(s, "%m/%d/%Y %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    return None

def _iso_week_id(dt: datetime) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"

# Normalize legacy terms into canonical language
PRIMARY_MAP = {
    "APPROVED_DRYRUN": "APPROVED_BUT_GATED",
}

# Noise filters (primary reasons / tokens)
NOISE_PRIMARY = {
    "SELF_TEST",
    "DAILY_SUMMARY",
    "SELF_TEST_POLICY_DENY (safe)",
}
NOISE_TOKEN = {"SYSTEM"}  # keep the system *functional* data out of weekly insight

def _clean_primary(p: str) -> str:
    p = (p or "").strip()
    return PRIMARY_MAP.get(p, p) if p else "UNKNOWN"

def _split_secondary(s: str) -> List[str]:
    s = (s or "").strip()
    if not s or s.lower() == "none":
        return []
    # common format: "STALE,IMMATURE" or "foo=1, bar=2"
    parts = [x.strip() for x in s.split(",") if x.strip()]
    return parts

def _is_noise_secondary(x: str) -> bool:
    xl = x.lower()
    # filter wiring/self-test artifacts and "explanatory text" that shouldn't be blockers
    if "wiring" in xl:
        return True
    if "if you can read this row" in xl:
        return True
    if "sheets access works" in xl:
        return True
    if xl.startswith("self_test"):
        return True
    if xl.startswith("daily_summary"):
        return True
    # also filter key=value counters that came from the daily summary row being re-ingested
    if "=" in x and any(k in xl for k in ["stale=", "unknown=", "self_test="]):
        return True
    return False

def _filter_rows(rows: List[Dict[str, Any]], start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        ts = _parse_ts_any(r.get("Timestamp"))
        if not ts:
            continue
        if ts < start_utc or ts > end_utc:
            continue

        token = str(r.get("Token") or "").strip()
        primary = str(r.get("Primary_Reason") or "").strip()

        # Drop known test/system noise from weekly digest
        if token in NOISE_TOKEN:
            # BUT keep SYSTEM rows if they are not noise primaries? For weekly digest, we exclude all SYSTEM by default.
            continue
        if primary in NOISE_PRIMARY:
            continue

        out.append(r)
    return out


# ---------- main digest ----------

def run_wnh_weekly_digest(force: bool = False) -> Dict[str, Any]:
    if not _enabled():
        return {"ok": True, "rows": 0, "skipped": True, "reason": "disabled"}

    now = _now_utc()
    week_id = _iso_week_id(now)
    decision_id = f"wnh_weekly_{week_id}"

    wnh_ws = _open_ws(_wnh_tab())
    rows = _rows_as_dicts(wnh_ws, tail_n=_tail_n())

    # Rolling window: last N days (inclusive)
    days = _window_days()
    start = now - timedelta(days=days)
    end = now

    filtered = _filter_rows(rows, start, end)

    # Counters
    stage_c = Counter()
    outcome_c = Counter()
    primary_c = Counter()
    secondary_c = Counter()

    for r in filtered:
        stage = str(r.get("Stage") or "").strip() or "UNKNOWN"
        outcome = str(r.get("Outcome") or "").strip() or "UNKNOWN"
        primary = _clean_primary(str(r.get("Primary_Reason") or ""))
        sec_list = _split_secondary(str(r.get("Secondary_Reasons") or ""))

        stage_c[stage] += 1
        outcome_c[outcome] += 1
        primary_c[primary] += 1

        for s in sec_list:
            s2 = _clean_primary(s)  # allow map here too
            if _is_noise_secondary(s2):
                continue
            secondary_c[s2] += 1

    top_primary = primary_c.most_common(8)
    top_secondary = secondary_c.most_common(10)

    # Build human story
    stage_str = json.dumps(dict(stage_c), separators=(",", ":"), ensure_ascii=False) if stage_c else "{}"
    outcome_str = json.dumps(dict(outcome_c), separators=(",", ":"), ensure_ascii=False) if outcome_c else "{}"
    prim_str = ", ".join([f"{k}={v}" for k, v in top_primary]) if top_primary else "none"
    sec_str = ", ".join([f"{k}={v}" for k, v in top_secondary]) if top_secondary else "none"

    story = (
        f"WNH Weekly Digest (UTC {start.date()}→{end.date()}): rows={len(filtered)} "
        f"| stages={stage_str} | outcomes={outcome_str} | Primary={prim_str} | Secondary={sec_str}"
    )

    payload = {
        "week_id": week_id,
        "window_utc": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "days": days,
        },
        "rows": len(filtered),
        "stage_counts": dict(stage_c),
        "outcome_counts": dict(outcome_c),
        "top_primary_reasons": top_primary,
        "top_secondary_reasons": top_secondary,
        "filters": {
            "drop_tokens": sorted(list(NOISE_TOKEN)),
            "drop_primary": sorted(list(NOISE_PRIMARY)),
            "primary_map": PRIMARY_MAP,
        },
    }

    target_tab = _target_tab()
    t_ws = _open_ws(target_tab)

    # Idempotency: skip if decision_id already exists in recent tail (unless force)
    if not force:
        tail = _rows_as_dicts(t_ws, tail_n=800)
        for r in tail:
            if str(r.get("decision_id") or "").strip() == decision_id:
                return {"ok": True, "rows": 0, "skipped": True, "reason": "already_emitted", "decision_id": decision_id, "week_id": week_id}

    # Map to Council_Insight headers (fill what exists; header-driven append does the right thing)
    row = {
        "Timestamp": _now_ts_str_localish(),
        "decision_id": decision_id,
        "Autonomy": "wnh_weekly_digest",
        "OK": "TRUE",
        "Reason": "WNH_WEEKLY_DIGEST",
        "Story": story,
        "Ash's Lens": "clean",
        "Soul": "0",
        "Nova": "0",
        "Orion": "0",
        "Ash": "0",
        "Lumen": "0",
        "Vigil": "0",
        "Raw Intent": json.dumps(payload, ensure_ascii=False),
        "Flags": json.dumps(["wnh_weekly", week_id]),
        "Outcome Tag": "WNH_WEEKLY",
    }

    res = _append_row_dict(target_tab, row)
    if not res.get("ok"):
        return res

    return {"ok": True, "rows": 1, "decision_id": decision_id, "week_id": week_id, "tab": target_tab}


if __name__ == "__main__":
    print(run_wnh_weekly_digest(force=True))
