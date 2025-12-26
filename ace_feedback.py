# ace_feedback.py
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

ACE_ENABLED = os.getenv("ACE_FEEDBACK_ENABLED", "0") == "1"
ACE_MODE = os.getenv("ACE_FEEDBACK_MODE", "log_only").strip().lower()  # log_only | soft_weight
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
        from sheets_gateway import SheetsGateway  # type: ignore
        mod["SheetsGateway"] = SheetsGateway
    except Exception:
        mod["SheetsGateway"] = None
    try:
        from utils import get_sheet, ensure_worksheet  # type: ignore
        mod["get_sheet"] = get_sheet
        mod["ensure_worksheet"] = ensure_worksheet
    except Exception:
        mod["get_sheet"] = None
        mod["ensure_worksheet"] = None
    return mod

_IMP = _try_imports()

def _open_sheet():
    if _IMP.get("SheetsGateway"):
        return _IMP["SheetsGateway"]()
    if _IMP.get("get_sheet"):
        return _IMP["get_sheet"]()
    raise RuntimeError("No Sheets adapter found.")

def _get_records(sheet, ws_name: str):
    if hasattr(sheet, "get_records_cached"):
        return sheet.get_records_cached(ws_name)  # type: ignore
    ws = sheet.worksheet(ws_name)
    return ws.get_all_records()

def _ensure_ws(sheet, ws_name: str, headers):
    if hasattr(sheet, "ensure_worksheet"):
        sheet.ensure_worksheet(ws_name, headers=headers, min_rows=2000, min_cols=len(headers) + 4)  # type: ignore
        return
    if _IMP.get("ensure_worksheet"):
        _IMP["ensure_worksheet"](sheet, ws_name, headers=headers)
        return
    try:
        ws = sheet.worksheet(ws_name)
        if ws.row_values(1) != headers:
            ws.clear()
            ws.append_row(headers)
    except Exception:
        ws = sheet.add_worksheet(title=ws_name, rows=2000, cols=len(headers) + 4)
        ws.append_row(headers)

def _append_row(sheet, ws_name: str, row):
    if hasattr(sheet, "append_row"):
        sheet.append_row(ws_name, row)  # type: ignore
        return
    ws = sheet.worksheet(ws_name)
    ws.append_row(row)

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

def read_ace_multipliers(sheet=None) -> Dict[str, float]:
    """
    Reads Council_Performance and returns {voice: multiplier}.
    Expects either:
      - columns: Voice, ACE_Weight_Multiplier
      - or: Voice, ACE_Multiplier
    """
    sheet = sheet or _open_sheet()
    rows = _get_records(sheet, COUNCIL_PERF_WS)
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
    """
    Cached multipliers + EMA-smoothed multipliers.
    Returns (raw, ema).
    """
    now = int(time.time())
    if _CACHE["raw"] is not None and (now - _CACHE["ts"] < ACE_TTL):
        return _CACHE["raw"], _CACHE["ema"]

    sheet = sheet or _open_sheet()
    raw = read_ace_multipliers(sheet=sheet)

    # Smooth into EMA, with per-refresh step limit.
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
    """
    Applies ACE multipliers (if enabled) to council_weights.
    - Fail-open: if read fails, returns original weights
    - Transparent: stamps meta with details
    """
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
        # fail-open
        return council_weights

    if ACE_MODE == "log_only":
        return council_weights

    # soft_weight mode
    out = {}
    clamp_hits = []
    for voice, w in council_weights.items():
        m = float(ema.get(voice, 1.0))
        neww = float(w) * m
        out[voice] = neww
        if m <= ACE_MIN + 1e-9 or m >= ACE_MAX - 1e-9:
            clamp_hits.append(voice)

    meta["ace"]["applied"] = True
    if clamp_hits:
        meta["ace"]["clamped_voices"] = clamp_hits

    # optional sheet log
    if ACE_LOG_ENABLED:
        try:
            sheet = sheet or _open_sheet()
            _ensure_ws(
                sheet,
                ACE_LOG_WS,
                headers=["Timestamp", "decision_id", "applied", "mode", "multipliers_ema", "notes"],
            )
            _append_row(
                sheet,
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
