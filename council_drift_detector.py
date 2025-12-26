# council_drift_detector.py (Phase 21.6) — Bus
# Drift/disagreement detection with minimal noise.
#
# IMPORTANT:
# - Uses utils.get_sheet() (gspread workbook) — does NOT instantiate SheetsGateway.
# - Fail-open: never crashes the scheduler loop.
# - Creates Council_Drift tab if missing.

import os
from datetime import datetime
from typing import Dict, List, Optional

# ---- Config ----
DRIFT_WS = os.getenv("COUNCIL_DRIFT_WS", "Council_Drift")
DECISION_ANALYTICS_WS = os.getenv("DECISION_ANALYTICS_WS", "Decision_Analytics")
COUNCIL_INSIGHT_WS = os.getenv("COUNCIL_INSIGHT_WS", "Council_Insight")

COUNCIL_DRIFT_ENABLED = os.getenv("COUNCIL_DRIFT_ENABLED", "0") == "1"
WINDOW_N = int(os.getenv("COUNCIL_DRIFT_WINDOW_N", "200"))

THRESH_P95 = float(os.getenv("COUNCIL_DRIFT_DISAGREE_P95", "0.55"))
THRESH_SHIFT = float(os.getenv("COUNCIL_DRIFT_SHIFT_RATE", "0.35"))
SUCCESS_MIN = float(os.getenv("COUNCIL_DRIFT_EXEC_SUCCESS_MIN", "0.75"))

DRIFT_ALERTS_ENABLED = os.getenv("DRIFT_ALERTS_ENABLED", "0") == "1"
DRIFT_ALERTS_DEDUP_MIN = int(os.getenv("DRIFT_ALERTS_DEDUP_MIN", "60"))

HEADERS = [
    "Timestamp",
    "Window_N",
    "Disagreement_Mean",
    "Disagreement_P95",
    "Majority_Voice_Mode",
    "Majority_Shift_Rate",
    "Exec_Success_Rate",
    "Drift_Flags",
    "Notes",
]

# ---- Imports from your Bus (safe fallbacks) ----
def _try_imports():
    mod = {}
    try:
        from utils import get_sheet  # type: ignore
        mod["get_sheet"] = get_sheet
    except Exception:
        mod["get_sheet"] = None

    try:
        from utils import telegram_send_deduped  # type: ignore
        mod["telegram_send_deduped"] = telegram_send_deduped
    except Exception:
        mod["telegram_send_deduped"] = None

    return mod

_IMP = _try_imports()


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace("%", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _pct95(values: List[float]) -> Optional[float]:
    if not values:
        return None
    v = sorted(values)
    idx = int(round(0.95 * (len(v) - 1)))
    return v[max(0, min(idx, len(v) - 1))]


def _mode(values: List[str]) -> str:
    if not values:
        return ""
    counts: Dict[str, int] = {}
    for x in values:
        counts[x] = counts.get(x, 0) + 1
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def _shift_rate(values: List[str]) -> float:
    if len(values) < 2:
        return 0.0
    shifts = 0
    for i in range(1, len(values)):
        if values[i] != values[i - 1]:
            shifts += 1
    return shifts / float(len(values) - 1)


def _boolish_ok(x) -> Optional[bool]:
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in ("true", "yes", "1", "ok", "success", "passed"):
        return True
    if s in ("false", "no", "0", "fail", "error", "failed"):
        return False
    return None


def _open_sheet():
    if not _IMP.get("get_sheet"):
        raise RuntimeError("utils.get_sheet not available")
    return _IMP["get_sheet"]()


def _ensure_ws(sheet, ws_name: str, headers: List[str]):
    # gspread workbook object
    try:
        ws = sheet.worksheet(ws_name)
        existing = ws.row_values(1)
        if existing != headers:
            ws.clear()
            ws.append_row(headers)
        return
    except Exception:
        # create and seed headers
        ws = sheet.add_worksheet(title=ws_name, rows=2000, cols=max(20, len(headers) + 4))
        ws.append_row(headers)


def _get_records(sheet, ws_name: str) -> List[dict]:
    ws = sheet.worksheet(ws_name)
    return ws.get_all_records()


def _append_row(sheet, ws_name: str, row: List):
    ws = sheet.worksheet(ws_name)
    ws.append_row(row)


def _send_alert(text: str, dedup_key: str):
    if not DRIFT_ALERTS_ENABLED:
        return
    fn = _IMP.get("telegram_send_deduped")
    if not fn:
        return
    ttl = DRIFT_ALERTS_DEDUP_MIN * 60
    try:
        fn(text, dedup_key, ttl_sec=ttl)  # type: ignore
    except Exception:
        pass


def run_council_drift_detector() -> Dict:
    """
    Phase 21.6
    - Reads Decision_Analytics (preferred) else Council_Insight
    - Computes rolling disagreement + majority stability + exec success
    - Appends one row to Council_Drift
    - Optional Telegram ping when flags trip
    """
    if not COUNCIL_DRIFT_ENABLED:
        return {"ok": True, "skipped": True, "reason": "COUNCIL_DRIFT_ENABLED=0"}

    sheet = _open_sheet()
    _ensure_ws(sheet, DRIFT_WS, HEADERS)

    # Prefer Decision_Analytics
    source = DECISION_ANALYTICS_WS
    try:
        rows = _get_records(sheet, DECISION_ANALYTICS_WS)
    except Exception:
        source = COUNCIL_INSIGHT_WS
        rows = _get_records(sheet, COUNCIL_INSIGHT_WS)

    if not rows:
        return {"ok": True, "skipped": True, "reason": f"No rows in {source}"}

    window = rows[-WINDOW_N:] if len(rows) > WINDOW_N else rows

    disagreements: List[float] = []
    majorities: List[str] = []
    exec_ok: List[bool] = []

    for r in window:
        d = _safe_float(r.get("Disagreement_Index") or r.get("Disagreement") or r.get("DisagreementIndex"))
        if d is not None:
            disagreements.append(d)

        mv = str(r.get("Majority_Voice") or r.get("Majority") or "").strip()
        if mv:
            majorities.append(mv)

        ok = _boolish_ok(r.get("Exec_OK") or r.get("Execution_OK") or r.get("OK") or r.get("Status"))
        if ok is not None:
            exec_ok.append(ok)

    disagree_mean = round(sum(disagreements) / len(disagreements), 4) if disagreements else ""
    disagree_p95 = round((_pct95(disagreements) or 0.0), 4) if disagreements else ""

    majority_mode = _mode(majorities)
    majority_shift = round(_shift_rate(majorities), 4)

    success_rate = ""
    if exec_ok:
        success_rate = round(sum(1 for x in exec_ok if x) / float(len(exec_ok)), 4)

    flags: List[str] = []
    notes: List[str] = []

    if isinstance(disagree_p95, float) and disagree_p95 > THRESH_P95:
        flags.append("high_disagreement")
        notes.append(f"p95={disagree_p95} > {THRESH_P95}")

    if majority_shift > THRESH_SHIFT:
        flags.append("majority_flapping")
        notes.append(f"shift_rate={majority_shift} > {THRESH_SHIFT}")

    if isinstance(success_rate, float) and success_rate < SUCCESS_MIN:
        flags.append("execution_drop")
        notes.append(f"exec_success={success_rate} < {SUCCESS_MIN}")

    if len(majorities) >= 100:
        base_mode = _mode(majorities[:50])
        now_mode = _mode(majorities[-50:])
        if base_mode and now_mode and base_mode != now_mode:
            flags.append("voice_shift")
            notes.append(f"mode {base_mode}->{now_mode}")

    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    out_row = [
        ts,
        int(len(window)),
        disagree_mean,
        disagree_p95,
        majority_mode,
        majority_shift,
        success_rate,
        ",".join(flags),
        " | ".join(notes)[:240],
    ]

    _append_row(sheet, DRIFT_WS, out_row)

    if flags:
        _send_alert(
            "🧭 Council Drift Detected\n"
            f"- flags: {', '.join(flags)}\n"
            f"- disagree_p95: {disagree_p95}\n"
            f"- majority_shift: {majority_shift}\n"
            f"- exec_success: {success_rate}\n"
            f"- window: {len(window)}",
            dedup_key=f"council_drift:{','.join(flags)}",
        )

    return {"ok": True, "source": source, "window": len(window), "flags": flags}
