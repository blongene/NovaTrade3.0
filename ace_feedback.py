# ace_feedback.py (Phase 21.7) — Bus
# Reads Council_Performance multipliers and optionally applies bounded/smoothed influence.
#
# IMPORTANT:
# - Uses utils.get_sheet() (gspread workbook) — does NOT instantiate SheetsGateway.
# - Fail-open: if any read fails, returns original weights.

import json
import os
import time
from typing import Dict, Optional, Tuple

ACE_ENABLED = os.getenv("ACE_FEEDBACK_ENABLED", "0") == "1"
ACE_MODE = (os.getenv("ACE_FEEDBACK_MODE", "log_only") or "log_only").strip().lower()  # log_only | soft_weight
ACE_TTL = int(os.getenv("ACE_FEEDBACK_TTL_SEC", "1800"))
ACE_MIN = float(os.getenv("ACE_FEEDBACK_MIN", "0.90"))
ACE_MAX = float(os.getenv("ACE_FEEDBACK_MAX", "1.15"))
ACE_ALPHA = float(os.getenv("ACE_FEEDBACK_EMA_ALPHA", "0.35"))
ACE_MAX_STEP = float(os.getenv("ACE_FEEDBACK_MAX_STEP", "0.02"))
ACE_LOG_ENABLED = os.getenv("ACE_FEEDBACK_LOG_ENABLED", "1") == "1"

COUNCIL_PERF_WS = os.getenv("COUNCIL_PERFORMANCE_WS", "Council_Performance")
ACE_LOG_WS = os.getenv("ACE_FEEDBACK_LOG_WS", "ACE_Feedback_Log")

_CACHE = {"ts": 0, "raw": None, "ema": None}

def _try_imports():
    mod = {}
    try:
        from utils import get_sheet  # type: ignore
        mod["get_sheet"] = get_sheet
    except Exception:
        mod["get_sheet"] = None
    return mod

_IMP = _try_imports()

def _open_sheet():
    if not _IMP.get("get_sheet"):
        raise RuntimeError("utils.get_sheet not available")
    return _IMP["get_sheet"]()

def _clamp(v: float) -> float:
    return max(ACE_MIN, min(ACE_MAX, v))

def _step_limit(prev: float, nxt: float) -> float:
    if ACE_MAX_STEP <= 0:
        return nxt
    delta = nxt - prev
    if abs(delta) <= ACE_MAX_STEP:
        return nxt
    return prev + (ACE_MAX_STEP if delta > 0 else -ACE_MAX_STEP)

def _ema(prev: float, raw: float) -> float:
    return (ACE_ALPHA * raw) + ((1.0 - ACE_ALPHA) * prev)

def _ensure_ws(sheet, ws_name: str, headers):
    try:
        ws = sheet.worksheet(ws_name)
        existing = ws.row_values(1)
        if existing != headers:
            ws.clear()
            ws.append_row(headers)
        return
    except Exception:
        ws = sheet.add_worksheet(title=ws_name, rows=2000, cols=max(20, len(headers) + 4))
        ws.append_row(headers)

def _append_row(sheet, ws_name: str, row):
    ws = sheet.worksheet(ws_name)
    ws.append_row(row)

def read_ace_multipliers(sheet=None) -> Dict[str, float]:
    sheet = sheet or _open_sheet()
    ws = sheet.worksheet(COUNCIL_PERF_WS)
    rows = ws.get_all_records()

    out: Dict[str, float] = {}
    for r in rows:
        voice = str(r.get("Voice") or r.get("Council_Voice") or "").strip()
        if not voice:
            continue
        raw = r.get("ACE_Weight_Multiplier") or r.get("ACE_Multiplier") or r.get("ACE") or ""
        try:
            v = float(str(raw).strip())
        except Exception:
            continue
        out[voice] = _clamp(v)
    return out

def get_ace_state(sheet=None) -> Tuple[Dict[str, float], Dict[str, float]]:
    now = int(time.time())
    if _CACHE["raw"] is not None and (now - _CACHE["ts"] < ACE_TTL):
        return _CACHE["raw"], _CACHE["ema"]

    sheet = sheet or _open_sheet()
    raw = read_ace_multipliers(sheet=sheet)

    prev_ema = _CACHE["ema"] or {}
    ema_out: Dict[str, float] = {}

    for voice, rmult in raw.items():
        prev = float(prev_ema.get(voice, rmult))
        candidate = _ema(prev, rmult)
        candidate = _step_limit(prev, candidate)
        ema_out[voice] = _clamp(candidate)

    _CACHE["ts"] = now
    _CACHE["raw"] = raw
    _CACHE["ema"] = ema_out
    return raw, ema_out

def apply_ace_to_weights(
    council_weights: Dict[str, float],
    decision_id: Optional[str] = None,
    sheet=None,
    meta: Optional[Dict] = None
) -> Dict[str, float]:
    meta = meta if meta is not None else {}
    meta.setdefault("ace", {})
    meta["ace"]["enabled"] = ACE_ENABLED
    meta["ace"]["mode"] = ACE_MODE

    if not ACE_ENABLED:
        return council_weights

    try:
        raw, ema = get_ace_state(sheet=sheet)
        meta["ace"]["multipliers_raw"] = raw
        meta["ace"]["multipliers_ema"] = ema
    except Exception as e:
        meta["ace"]["error"] = str(e)[:180]
        return council_weights  # fail-open

    if ACE_MODE == "log_only":
        return council_weights

    out = {}
    clamp_hits = []
    for voice, w in council_weights.items():
        m = float(ema.get(voice, 1.0))
        out[voice] = float(w) * m
        if m <= ACE_MIN + 1e-9 or m >= ACE_MAX - 1e-9:
            clamp_hits.append(voice)

    meta["ace"]["applied"] = True
    if clamp_hits:
        meta["ace"]["clamped_voices"] = clamp_hits

    if ACE_LOG_ENABLED:
        try:
            sheet2 = sheet or _open_sheet()
            _ensure_ws(
                sheet2,
                ACE_LOG_WS,
                headers=["Timestamp", "decision_id", "applied", "mode", "multipliers_ema", "notes"],
            )
            _append_row(
                sheet2,
                ACE_LOG_WS,
                [
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                    decision_id or "",
                    "TRUE",
                    ACE_MODE,
                    json.dumps(ema, separators=(",", ":"), sort_keys=True),
                    "",
                ],
            )
        except Exception:
            pass

    return out
