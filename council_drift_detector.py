# council_drift_detector.py
# Phase 21.6 — Council Disagreement + Drift Detection
# VISIBILITY ONLY — no intent mutation

from __future__ import annotations
import os, time, statistics
from typing import Dict, Any, List
from utils import sheets_get_records_cached, sheets_append_rows

SHEET_URL = os.getenv("SHEET_URL")
DECISION_WS = os.getenv("DECISION_ANALYTICS_WS", "Decision_Analytics")
DRIFT_WS = os.getenv("COUNCIL_DRIFT_WS", "Council_Drift")

WINDOW_N = int(os.getenv("COUNCIL_DRIFT_WINDOW_N", "200"))
RECENT_N = int(os.getenv("COUNCIL_DRIFT_RECENT_N", "40"))

DISAGREE_P95 = float(os.getenv("COUNCIL_DRIFT_DISAGREE_P95", "0.55"))
SHIFT_RATE_MAX = float(os.getenv("COUNCIL_DRIFT_SHIFT_RATE", "0.35"))

ENABLED = os.getenv("COUNCIL_DRIFT_ENABLED", "1").lower() in {"1","true","yes"}

HEADERS = [
    "ts_utc",
    "window_n",
    "disagreement_mean",
    "disagreement_p95",
    "majority_shift_rate",
    "exec_success_rate",
    "drift_flags",
]

def _majority_voice(row: Dict[str, Any]) -> str:
    return row.get("Majority_Voice") or ""

def _success(row: Dict[str, Any]) -> bool:
    return str(row.get("Exec Status","")).lower() in {"filled","done","ok"}

def run_council_drift() -> Dict[str, Any]:
    if not ENABLED or not SHEET_URL:
        return {"ok": False, "skipped": True}

    rows = sheets_get_records_cached(SHEET_URL, DECISION_WS, limit=WINDOW_N)
    if len(rows) < RECENT_N:
        return {"ok": False, "reason": "insufficient_rows"}

    recent = rows[-RECENT_N:]

    disagreements = []
    majority = []
    successes = []

    for r in recent:
        try:
            disagreements.append(float(r.get("Disagreement_Index", 0)))
        except Exception:
            pass
        majority.append(_majority_voice(r))
        successes.append(_success(r))

    if not disagreements:
        return {"ok": False, "reason": "no_disagreement_data"}

    mean_d = statistics.mean(disagreements)
    p95_d = statistics.quantiles(disagreements, n=20)[-1]

    shifts = 0
    for i in range(1, len(majority)):
        if majority[i] and majority[i-1] and majority[i] != majority[i-1]:
            shifts += 1
    shift_rate = shifts / max(1, len(majority)-1)

    success_rate = sum(1 for s in successes if s) / max(1, len(successes))

    flags = []
    if p95_d >= DISAGREE_P95:
        flags.append("HIGH_DISAGREEMENT")
    if shift_rate >= SHIFT_RATE_MAX:
        flags.append("MAJORITY_DRIFT")

    row = [
        time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        RECENT_N,
        round(mean_d, 4),
        round(p95_d, 4),
        round(shift_rate, 4),
        round(success_rate, 4),
        ",".join(flags),
    ]

    sheets_append_rows(SHEET_URL, DRIFT_WS, [row])

    return {
        "ok": True,
        "mean_disagreement": mean_d,
        "p95_disagreement": p95_d,
        "shift_rate": shift_rate,
        "success_rate": success_rate,
        "flags": flags,
    }
