# council_drift_detector.py
# Phase 21.6 — Disagreement + Drift Detection (visibility-first)
# Uses existing env vars from NovaTrade3.0.txt

from __future__ import annotations
import os, time, statistics
from typing import Any, Dict, List, Optional

from utils import sheets_get_records_cached, sheets_append_rows

SHEET_URL = os.getenv("SHEET_URL", "")

# Existing env vars (do NOT rename)
ENABLED = os.getenv("COUNCIL_DRIFT_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
WINDOW_N = int(os.getenv("COUNCIL_DRIFT_WINDOW_N", "200"))
DISAGREE_P95_THRESH = float(os.getenv("COUNCIL_DRIFT_DISAGREE_P95", "0.55"))
SHIFT_RATE_THRESH = float(os.getenv("COUNCIL_DRIFT_SHIFT_RATE", "0.35"))
ALERTS_ENABLED = os.getenv("DRIFT_ALERTS_ENABLED", "0").lower() in {"1", "true", "yes", "on"}

# Sheet names (kept as stable defaults; no new env required)
DECISION_WS = "Decision_Analytics"
DRIFT_WS = "Council_Drift"

# Internal: how many most-recent rows to evaluate for drift snapshot
RECENT_N = min(40, max(10, WINDOW_N // 5))  # e.g., 200 -> 40

HEADERS = [
    "ts_utc",
    "recent_n",
    "disagreement_mean",
    "disagreement_p95",
    "majority_shift_rate",
    "exec_success_rate",
    "drift_flags",
]

def _ts_utc() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v in ("", None): return default
        return float(v)
    except Exception:
        return default

def _success(status: Any) -> bool:
    s = str(status or "").lower()
    return s in {"filled", "done", "ok"}

def run_council_drift_detector() -> Dict[str, Any]:
    """
    Appends one summary row to Council_Drift.
    Does not mutate decisions. Does not spam Telegram.
    """
    if not ENABLED or not SHEET_URL:
        return {"ok": False, "skipped": True}

    rows = sheets_get_records_cached(SHEET_URL, DECISION_WS, limit=WINDOW_N) or []
    if len(rows) < max(RECENT_N, 10):
        return {"ok": False, "reason": "insufficient_rows", "rows": len(rows)}

    recent = rows[-RECENT_N:]

    disagreements: List[float] = []
    majorities: List[str] = []
    successes: List[bool] = []

    for r in recent:
        disagreements.append(_f(r.get("Disagreement_Index", 0.0), 0.0))
        majorities.append(str(r.get("Majority_Voice") or ""))
        successes.append(_success(r.get("Exec Status") or r.get("Exec_Status") or ""))

    mean_d = statistics.mean(disagreements) if disagreements else 0.0
    # p95: if enough samples, use quantiles; else use max
    if len(disagreements) >= 20:
        p95_d = statistics.quantiles(disagreements, n=20)[-1]
    else:
        p95_d = max(disagreements) if disagreements else 0.0

    # majority shift rate (ignoring blank majorities)
    shifts = 0
    seen = 0
    for i in range(1, len(majorities)):
        if majorities[i] and majorities[i - 1]:
            seen += 1
            if majorities[i] != majorities[i - 1]:
                shifts += 1
    shift_rate = shifts / max(1, seen)

    success_rate = (sum(1 for s in successes if s) / max(1, len(successes)))

    flags: List[str] = []
    if p95_d >= DISAGREE_P95_THRESH:
        flags.append("HIGH_DISAGREEMENT")
    if shift_rate >= SHIFT_RATE_THRESH:
        flags.append("MAJORITY_DRIFT")

    out_row = [
        _ts_utc(),
        RECENT_N,
        round(mean_d, 6),
        round(p95_d, 6),
        round(shift_rate, 6),
        round(success_rate, 6),
        ",".join(flags),
    ]

    sheets_append_rows(SHEET_URL, DRIFT_WS, [out_row])

    # Alerts are intentionally a no-op here unless you already have a drift alert mechanism elsewhere.
    # This function stays visibility-first to avoid spam.
    return {
        "ok": True,
        "recent_n": RECENT_N,
        "mean_disagreement": mean_d,
        "p95_disagreement": p95_d,
        "shift_rate": shift_rate,
        "success_rate": success_rate,
        "flags": flags,
        "alerts_enabled": ALERTS_ENABLED,
    }
