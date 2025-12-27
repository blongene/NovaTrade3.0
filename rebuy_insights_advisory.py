# rebuy_insights_advisory.py — Phase 22A (advisory only)
# Writes "would rebuy" candidate insights to Rebuy_Insights WITHOUT enqueuing trades.

import os, time
from typing import Any, Dict, List, Tuple

from utils import (
    get_records_cached,
    ensure_sheet_headers,
    get_ws_cached,
    ws_append_row,
    to_float,
    warn,
)

TAB = os.getenv("REBUY_INSIGHTS_TAB", "Rebuy_Insights")

# Defaults intentionally conservative
ENABLED = os.getenv("REBUY_INSIGHTS_ADVISORY_ENABLED", "0").lower() in {"1","true","yes","on"}
MAX_ROWS = int(os.getenv("REBUY_INSIGHTS_MAX_ROWS", "25"))
THRESH_RATIO = float(os.getenv("REBUY_UNDERSIZED_THRESH_PCT", "0.5"))  # current < 50% of target
TTL_SEC = int(os.getenv("REBUY_INSIGHTS_TTL_SEC", "3600"))  # de-dupe per token within TTL


def _now_utc() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _headers() -> List[str]:
    return [
        "Timestamp",
        "Token",
        "Venue",
        "Current %",
        "Target %",
        "Deficit",
        "Ratio",
        "Score",
        "Reason",
        "Constraints",
        "Advisory",
        "Phase",
    ]

def _dedupe_recent(existing: List[Dict[str, Any]]) -> Dict[str, float]:
    """Return token->last_ts_epoch for recent advisory rows."""
    out: Dict[str, float] = {}
    for r in existing[-300:]:
        if str(r.get("Advisory","")).strip().lower() not in {"true","1","yes"}:
            continue
        tok = str(r.get("Token","")).strip().upper()
        ts  = str(r.get("Timestamp","")).strip()
        if not tok or not ts:
            continue
        try:
            # Timestamp is UTC string "%Y-%m-%d %H:%M:%S"
            t = time.mktime(time.strptime(ts, "%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue
        if tok not in out or t > out[tok]:
            out[tok] = t
    return out

def run_rebuy_insights_advisory() -> None:
    if not ENABLED:
        return

    # Ensure headers (no sheet churn if already present)
    ensure_sheet_headers(TAB, _headers())

    # Read Rotation_Stats as the main source-of-truth for % columns (if populated)
    stats = get_records_cached("Rotation_Stats", ttl_s=120) or []

    # Read existing insights for de-dupe
    existing = get_records_cached(TAB, ttl_s=120) or []
    recent = _dedupe_recent(existing)
    now_epoch = time.time()

    # If Rotation_Stats isn't ready yet, write a single breadcrumb row explaining why
    if not stats:
        _append_breadcrumb("Rotation_Stats empty; rebuy advisory has no inputs yet.")
        return

    # Detect expected columns (tolerate schema variants)
    # Common variants: "Current %", "Current%", "Current Pct", etc.
    def col_get(r: Dict[str, Any], keys: List[str]) -> Any:
        for k in keys:
            if k in r:
                return r.get(k)
        return None

    candidates: List[Tuple[str, float, float, float, float]] = []
    for r in stats:
        tok = str(r.get("Token","") or r.get("Asset","")).strip().upper()
        if not tok:
            continue
        cur = to_float(col_get(r, ["Current %","Current%","Current Pct","CurrentPct","Current Percent","Current"])) or 0.0
        tgt = to_float(col_get(r, ["Target %","Target%","Target Pct","TargetPct","Target Percent","Target"])) or 0.0
        if tgt <= 0:
            continue
        ratio = (cur / tgt) if tgt else 1.0
        deficit = max(tgt - cur, 0.0)
        if ratio < THRESH_RATIO and deficit > 0:
            candidates.append((tok, cur, tgt, deficit, ratio))

    if not candidates:
        _append_breadcrumb("No undersized positions found (by Rotation_Stats Current% vs Target%).")
        return

    # Sort largest deficit first
    candidates.sort(key=lambda x: x[3], reverse=True)

    ws = get_ws_cached(TAB, ttl_s=60)
    written = 0
    for tok, cur, tgt, deficit, ratio in candidates:
        if written >= MAX_ROWS:
            break
        last = recent.get(tok)
        if last and (now_epoch - last) < TTL_SEC:
            continue

        score = round((1.0 - ratio) * 100.0, 3)  # simple, interpretable score
        reason = f"Undersized vs target (ratio={ratio:.3f} < {THRESH_RATIO:.3f})"
        constraints = "no_exec;advisory_only"

        row = [
            _now_utc(),
            tok,
            "",  # venue unknown at this layer; execution/router decides later
            cur,
            tgt,
            round(deficit, 6),
            round(ratio, 6),
            score,
            reason,
            constraints,
            "TRUE",
            "22A",
        ]
        try:
            ws_append_row(ws, row)
            written += 1
        except Exception as e:
            warn(f"Rebuy_Insights append failed: {e}")
            break

    if written == 0:
        _append_breadcrumb("Candidates exist but dedupe TTL suppressed writes (normal).")
    else:
        _append_breadcrumb(f"Wrote {written} advisory rebuy insight row(s).", advisory=False)

def _append_breadcrumb(msg: str, advisory: bool = True) -> None:
    """Write a single breadcrumb row with an explanation (kept sparse)."""
    try:
        ws = get_ws_cached(TAB, ttl_s=60)
        row = [
            _now_utc(),
            "", "",
            "", "", "", "", "",
            msg,
            "no_exec;breadcrumb",
            "TRUE" if advisory else "FALSE",
            "22A",
        ]
        ws_append_row(ws, row)
    except Exception as e:
        warn(f"Rebuy_Insights breadcrumb failed: {e}")
