# wnh_weekly_digest.py
"""
WNH → Council_Insight Weekly Digest (Bus/DB-driven; Sheets-mirrored)

What it does
------------
- Reads Why_Nothing_Happened (WNH) rows from Sheets (presentation plane)
- Aggregates the last N days (default 7) into a weekly digest
- Writes a single row into Council_Insight with:
  - Stage/Outcome counts
  - Top primary + secondary reasons
  - Token leaderboard (top tokens by WNH appearances)

Scheduling
----------
Safe to schedule daily from main.py. This module self-gates:
- Only runs on configured weekday (default: Thursday)
- Dedupe by week_id + decision_id unless force=True

Config (DB_READ_JSON)
---------------------
{
  "wnh": {
    "tab": "Why_Nothing_Happened",
    "weekly_digest": {
      "enabled": 1,
      "days": 7,
      "dow": "thu",                 # mon,tue,wed,thu,fri,sat,sun
      "drop_tokens": ["SYSTEM"],
      "drop_primary": ["DAILY_SUMMARY","SELF_TEST","SELF_TEST_POLICY_DENY (safe)"],
      "primary_map": {"APPROVED_DRYRUN":"APPROVED_BUT_GATED"},
      "leaderboard_n": 10,
      "council_insight_tab": "Council_Insight"
    }
  }
}
"""

from __future__ import annotations

import os
import json
import time
import uuid
import logging
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict


log = logging.getLogger("wnh_weekly_digest")


# -----------------------------
# small helpers
# -----------------------------

def _truthy(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _load_db_read_json() -> dict:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg_get(cfg: dict, dotted: str, default=None):
    cur = cfg
    for p in dotted.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return default if cur is None else cur


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_week_id(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _dow_str(dt: datetime) -> str:
    # mon..sun
    return ["mon", "tue", "wed", "thu", "fri", "sat", "sun"][dt.weekday()]


def _parse_ts(s: str) -> datetime | None:
    """
    Accepts either:
      - 2026-01-22 00:52:50
      - 1/22/2026 13:07:19
      - 01/22/2026 13:07:19
      - 2026-01-22T00:52:50Z (rare)
    Returns UTC-aware datetime.
    """
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None

    # Normalize ISO-ish Z
    if s.endswith("Z") and "T" in s:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    # Last resort: try fromisoformat without timezone
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _retry(callable_fn, *, tries: int = 6, base_sleep: float = 0.6):
    """
    Minimal exponential backoff wrapper for gspread calls.
    """
    last = None
    for i in range(tries):
        try:
            return callable_fn()
        except Exception as e:
            last = e
            # jitterless but increasing
            time.sleep(base_sleep * (2 ** i))
    raise last  # type: ignore


def _get_ws(tab: str):
    """
    Prefer your cached helper if present; fallback to gspread direct.
    """
    # 1) Preferred: utils.get_ws_cached (you already use this pattern elsewhere)
    try:
        from utils import get_ws_cached  # type: ignore
        return get_ws_cached(tab, ttl_s=30)
    except Exception:
        pass

    # 2) Fallback: direct gspread
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
        return sh.add_worksheet(title=tab, rows=4000, cols=60)


def _read_header(ws) -> list[str]:
    try:
        return _retry(lambda: ws.row_values(1)) or []
    except Exception:
        vals = _retry(lambda: ws.get_all_values()) or []
        return vals[0] if vals else []


def _append_by_header(ws, row_dict: dict) -> None:
    header = _read_header(ws)
    if not header:
        raise RuntimeError("Target sheet has no header row")
    row = [row_dict.get(h, "") for h in header]
    try:
        _retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))
    except Exception:
        _retry(lambda: ws.append_row(row))


def _dedupe_recent(ws, decision_id: str, window: int = 250) -> bool:
    """
    Returns True if decision_id already exists in recent rows.
    (Lightweight: reads all values then slices tail; OK because Council_Insight is modest.)
    """
    vals = _retry(lambda: ws.get_all_values()) or []
    if len(vals) <= 1:
        return False
    header = vals[0]
    try:
        didx = header.index("decision_id")
    except Exception:
        # if no decision_id column, can't dedupe reliably
        return False

    tail = vals[max(1, len(vals) - window):]
    for r in tail:
        if len(r) > didx and r[didx] == decision_id:
            return True
    return False


def _compact_pairs(pairs: list[tuple[str, int]], limit: int = 6) -> str:
    if not pairs:
        return "none"
    out = []
    for k, n in pairs[:limit]:
        out.append(f"{k}={n}")
    return ", ".join(out)


def _format_token_leaderboard(lb_rows: list[dict], limit: int = 10) -> str:
    """
    Example: XYZ(12: B=9 D=3) top=STALE; TESTTRADE(7: B=0 D=7) top=APPROVED_BUT_GATED
    """
    if not lb_rows:
        return "none"
    chunks = []
    for r in lb_rows[:limit]:
        tok = r["token"]
        total = r["total"]
        b = r.get("blocked", 0)
        d = r.get("deferred", 0)
        top_reason = r.get("top_primary", "")
        if top_reason:
            chunks.append(f"{tok}({total}: B={b} D={d}) top={top_reason}")
        else:
            chunks.append(f"{tok}({total}: B={b} D={d})")
    return " | ".join(chunks)


# -----------------------------
# public entrypoint
# -----------------------------

def run_wnh_weekly_digest(force: bool = False) -> dict:
    cfg = _load_db_read_json()

    enabled = _truthy(_cfg_get(cfg, "wnh.weekly_digest.enabled", 1))
    if not enabled and not force:
        return {"ok": True, "rows": 0, "skipped": "disabled"}

    days = int(_cfg_get(cfg, "wnh.weekly_digest.days", 7) or 7)
    dow = str(_cfg_get(cfg, "wnh.weekly_digest.dow", "thu") or "thu").strip().lower()

    drop_tokens = _cfg_get(cfg, "wnh.weekly_digest.drop_tokens", ["SYSTEM"]) or ["SYSTEM"]
    drop_primary = _cfg_get(cfg, "wnh.weekly_digest.drop_primary", ["DAILY_SUMMARY", "SELF_TEST", "SELF_TEST_POLICY_DENY (safe)"]) or []
    primary_map = _cfg_get(cfg, "wnh.weekly_digest.primary_map", {"APPROVED_DRYRUN": "APPROVED_BUT_GATED"}) or {}
    leaderboard_n = int(_cfg_get(cfg, "wnh.weekly_digest.leaderboard_n", 10) or 10)

    wnh_tab = str(_cfg_get(cfg, "wnh.tab", "Why_Nothing_Happened") or "Why_Nothing_Happened").strip() or "Why_Nothing_Happened"
    council_tab = str(_cfg_get(cfg, "wnh.weekly_digest.council_insight_tab", "Council_Insight") or "Council_Insight").strip() or "Council_Insight"

    now = _now_utc()
    # self-gate by weekday unless force
    if not force and dow and _dow_str(now) != dow:
        return {"ok": True, "rows": 0, "skipped": f"dow_mismatch (now={_dow_str(now)} want={dow})"}

    window_end = now
    window_start = now - timedelta(days=days)

    week_id = _iso_week_id(now)
    decision_id = f"wnh_weekly_{week_id}"

    # Load WNH sheet
    ws_wnh = _get_ws(wnh_tab)
    vals = _retry(lambda: ws_wnh.get_all_values()) or []
    if len(vals) <= 1:
        # No data rows; still emit a digest if force, otherwise skip quietly.
        if not force:
            return {"ok": True, "rows": 0, "skipped": "no_wnh_rows"}
        header = vals[0] if vals else []
        # proceed with empty aggregates

    header = vals[0] if vals else []
    rows = vals[1:] if len(vals) > 1 else []

    # Column indices (best effort)
    def idx(name: str) -> int | None:
        try:
            return header.index(name)
        except Exception:
            return None

    i_ts = idx("Timestamp")
    i_token = idx("Token")
    i_stage = idx("Stage")
    i_outcome = idx("Outcome")
    i_primary = idx("Primary_Reason")
    i_secondary = idx("Secondary_Reasons")

    # Aggregate
    stage_counts = Counter()
    outcome_counts = Counter()
    primary_counts = Counter()
    secondary_counts = Counter()

    token_total = Counter()
    token_blocked = Counter()
    token_deferred = Counter()
    token_primary = defaultdict(Counter)  # token -> Counter(primary)

    considered_rows = 0

    for r in rows:
        # timestamp filter
        ts_val = r[i_ts] if (i_ts is not None and len(r) > i_ts) else ""
        ts = _parse_ts(ts_val)
        if not ts:
            continue
        if ts < window_start or ts > window_end:
            continue

        tok = (r[i_token] if (i_token is not None and len(r) > i_token) else "").strip()
        if tok in set(drop_tokens):
            continue

        primary = (r[i_primary] if (i_primary is not None and len(r) > i_primary) else "").strip()
        if primary in set(drop_primary):
            continue
        if primary in primary_map:
            primary = str(primary_map[primary])

        stage = (r[i_stage] if (i_stage is not None and len(r) > i_stage) else "").strip()
        outcome = (r[i_outcome] if (i_outcome is not None and len(r) > i_outcome) else "").strip()
        secondary = (r[i_secondary] if (i_secondary is not None and len(r) > i_secondary) else "").strip()

        considered_rows += 1

        if stage:
            stage_counts[stage] += 1
        if outcome:
            outcome_counts[outcome] += 1
        if primary:
            primary_counts[primary] += 1

        if secondary and secondary.lower() not in ("none", "null"):
            for part in [p.strip() for p in secondary.split(",") if p.strip()]:
                secondary_counts[part] += 1

        if tok:
            token_total[tok] += 1
            if outcome.upper() == "BLOCKED":
                token_blocked[tok] += 1
            if outcome.upper() == "DEFERRED":
                token_deferred[tok] += 1
            if primary:
                token_primary[tok][primary] += 1

    top_primary = primary_counts.most_common(10)
    top_secondary = secondary_counts.most_common(10)

    # Build leaderboard objects
    lb = []
    for tok, total in token_total.most_common(leaderboard_n):
        tp = ""
        if token_primary.get(tok):
            tp = token_primary[tok].most_common(1)[0][0]
        lb.append({
            "token": tok,
            "total": int(total),
            "blocked": int(token_blocked.get(tok, 0)),
            "deferred": int(token_deferred.get(tok, 0)),
            "top_primary": tp,
        })

    # Story formatting (keep it readable in a single cell)
    start_s = window_start.strftime("%Y-%m-%d")
    end_s = window_end.strftime("%Y-%m-%d")
    story_lines = [
        f"WNH Weekly Digest (UTC {start_s}→{end_s}): rows={considered_rows}",
        f"Stages={json.dumps(dict(stage_counts), separators=(',',':'))}",
        f"Outcomes={json.dumps(dict(outcome_counts), separators=(',',':'))}",
        f"Primary={_compact_pairs(top_primary, limit=6)}",
        f"Secondary={_compact_pairs(top_secondary, limit=6)}",
        f"Token Leaderboard={_format_token_leaderboard(lb, limit=leaderboard_n)}",
    ]
    story = " | ".join([s for s in story_lines if s])

    raw_intent = {
        "week_id": week_id,
        "window_utc": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "days": days,
        },
        "rows": considered_rows,
        "stage_counts": dict(stage_counts),
        "outcome_counts": dict(outcome_counts),
        "top_primary_reasons": [[k, int(v)] for k, v in top_primary],
        "top_secondary_reasons": [[k, int(v)] for k, v in top_secondary],
        "token_leaderboard": lb,
        "filters": {
            "drop_tokens": drop_tokens,
            "drop_primary": drop_primary,
            "primary_map": primary_map,
        },
        "source": "wnh_weekly_digest",
    }

    # Write to Council_Insight
    ws_council = _get_ws(council_tab)

    if not force and _dedupe_recent(ws_council, decision_id):
        return {"ok": True, "rows": 0, "decision_id": decision_id, "week_id": week_id, "tab": council_tab, "deduped": True}

    ts_out = _now_utc().strftime("%m/%d/%Y %H:%M:%S")

    row_dict = {
        "Timestamp": ts_out,
        "decision_id": decision_id,
        "Autonomy": "wnh_weekly_digest",
        "OK": "TRUE",
        "Reason": "WNH_WEEKLY_DIGEST",
        "Story": story,
        "Ash's Lens": "clean",
        "Raw Intent": json.dumps(raw_intent, separators=(",", ":"), sort_keys=True),
        "Flags": json.dumps(["wnh_weekly", week_id]),
        "Outcome Tag": "WNH_WEEKLY",
    }

    _append_by_header(ws_council, row_dict)

    # Best-effort: mirror append event (if you have it)
    try:
        from db_mirror import mirror_append  # type: ignore
        # We don't know the exact header order here; mirror_append expects [row] arrays typically.
        # Safer: mirror payload event instead if you have it; otherwise skip.
        pass
    except Exception:
        pass

    return {"ok": True, "rows": 1, "decision_id": decision_id, "week_id": week_id, "tab": council_tab}


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    print(run_wnh_weekly_digest(force=_truthy(os.getenv("FORCE"))))
