#!/usr/bin/env python3
"""
ace_bias.py
Phase 21 – ACE (Autonomous Council Engine) bias helpers.

Reads the Council_Performance sheet and produces per-voice
weight multipliers used to gently bias council weights.

Safe by design:
- If anything fails (env, Sheets, auth), it falls back to 1.0 for all voices.
- Only affects the "council" metadata / influence weights (soft), not hard trade policy.

This version is a DROP-IN replacement for your current ace_bias.py:
- Keeps existing env vars:
    COUNCIL_ACE_ENABLE, COUNCIL_ACE_WS, COUNCIL_ACE_CACHE_SECONDS, SHEET_URL
- Uses utils.get_gspread_client() if present (preferred, consistent auth/backoff),
  else falls back to your prior gspread+oauth2client loading.
- Adds bounded per-refresh step limiting to prevent oscillation.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Dict, Any, Optional

VOICE_KEYS = ("soul", "nova", "orion", "ash", "lumen", "vigil")

ACE_ENABLED = os.getenv("COUNCIL_ACE_ENABLE", "1").lower() in ("1", "true", "yes", "on")
SHEET_URL = os.getenv("SHEET_URL", "")
ACE_WS = os.getenv("COUNCIL_ACE_WS", "Council_Performance")
ACE_CACHE_SECONDS = int(os.getenv("COUNCIL_ACE_CACHE_SECONDS", "300"))

# Hard safety bounds (do NOT require new env vars)
# Keep your original "reasonable range" idea but tighten slightly for stability.
_MIN_MULT = 0.90
_MAX_MULT = 1.15

# Max movement allowed per refresh to prevent thrashing (no new env; conservative default)
_MAX_STEP = 0.02  # +/- 0.02 per cache refresh

_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {
    "ts": 0.0,
    "mult": {k: 1.0 for k in VOICE_KEYS},
    "last_mult": {k: 1.0 for k in VOICE_KEYS},
}

def _log(msg: str) -> None:
    try:
        print(f"[ace_bias] {msg}")
    except Exception:
        pass

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default

def _idx(headers: list, name: str) -> int:
    try:
        return headers.index(name)
    except ValueError:
        return -1

def _open_worksheet_prefer_utils():
    """
    Preferred: use your existing utils.get_gspread_client() so we inherit your
    service-account fallback logic and any rate-limits/backoff you already built.
    """
    try:
        from utils import get_gspread_client
        if not SHEET_URL:
            raise RuntimeError("SHEET_URL not configured")
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        return sh.worksheet(ACE_WS)
    except Exception:
        return None

def _open_worksheet_fallback():
    """
    Fallback: keep your previous behavior (gspread + oauth2client).
    """
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    if not SHEET_URL:
        raise RuntimeError("SHEET_URL not configured")

    svc = (
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("GOOGLE_CREDENTIALS_JSON")
        or os.getenv("SVC_JSON")
        or "sentiment-log-service.json"
    )

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    if str(svc).strip().startswith("{"):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(svc), scope)
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)

    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet(ACE_WS)

def _open_worksheet():
    ws = _open_worksheet_prefer_utils()
    if ws is not None:
        return ws
    return _open_worksheet_fallback()

def _compute_from_rates(row: list, col_trades: int, col_success: int, col_error: int) -> Optional[float]:
    """
    Optional path: if ACE_Weight_Multiplier column is missing, compute it.
    ACE_Score = clamp(0..1, 0.7*success_rate + 0.3*(1-error_rate))
    ACE_Mult  = clamp(MIN..MAX, 0.95 + 0.20*ACE_Score)
    """
    trades = int(_safe_float(row[col_trades], 0.0)) if col_trades >= 0 else 0
    if trades < 10:
        return None
    success = _safe_float(row[col_success], 0.0) if col_success >= 0 else 0.0
    error = _safe_float(row[col_error], 0.0) if col_error >= 0 else 0.0

    success = _clamp(success, 0.0, 1.0)
    error = _clamp(error, 0.0, 1.0)
    ace_score = _clamp(0.7 * success + 0.3 * (1.0 - error), 0.0, 1.0)
    raw = 0.95 + 0.20 * ace_score
    return _clamp(raw, _MIN_MULT, _MAX_MULT)

def _step_limit(new_mult: Dict[str, float], last_mult: Dict[str, float]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k in VOICE_KEYS:
        v_new = float(new_mult.get(k, 1.0))
        v_old = float(last_mult.get(k, 1.0))
        out[k] = _clamp(v_new, v_old - _MAX_STEP, v_old + _MAX_STEP)
    return out

def _refresh_cache_locked(now: float) -> None:
    """
    Reload ACE multipliers from Council_Performance into the cache.
    Primary expected columns: Voice, ACE_Weight_Multiplier
    Optional compute fallback: Voice, Trades, Success_Rate, Error_Rate
    """
    if not SHEET_URL:
        _log("SHEET_URL not set; ACE disabled.")
        return

    try:
        ws = _open_worksheet()
        values = ws.get_all_values() or []
        if not values:
            return

        headers = values[0]
        rows = values[1:]

        col_voice = _idx(headers, "Voice")
        col_mult = _idx(headers, "ACE_Weight_Multiplier")

        # Optional compute fallback columns:
        col_trades = _idx(headers, "Trades")
        col_success = _idx(headers, "Success_Rate")
        col_error = _idx(headers, "Error_Rate")

        if col_voice < 0:
            _log("Missing Voice column; ACE disabled.")
            return

        mult: Dict[str, float] = {k: 1.0 for k in VOICE_KEYS}

        for row in rows:
            if not row or len(row) <= col_voice:
                continue
            name = (row[col_voice] or "").strip().lower()
            if not name or name not in mult:
                continue

            val: Optional[float] = None

            # Preferred: use sheet-provided multiplier if present
            if col_mult >= 0 and len(row) > col_mult and row[col_mult] not in ("", None):
                try:
                    val = float(row[col_mult])
                except Exception:
                    val = None
            else:
                # Fallback: compute if rates columns exist
                if col_trades >= 0 and col_success >= 0 and col_error >= 0:
                    if len(row) > max(col_trades, col_success, col_error):
                        val = _compute_from_rates(row, col_trades, col_success, col_error)

            if val is None:
                continue

            mult[name] = _clamp(float(val), _MIN_MULT, _MAX_MULT)

        # Step limit vs last returned to prevent oscillation
        last = dict(_cache.get("last_mult") or {k: 1.0 for k in VOICE_KEYS})
        mult_limited = _step_limit(mult, last)

        _cache["ts"] = now
        _cache["mult"] = mult_limited
        _cache["last_mult"] = dict(mult_limited)

        _log(f"Refreshed ACE multipliers: {mult_limited}")
    except Exception as e:
        _log(f"Failed to refresh ACE cache: {e}")

def get_ace_multipliers() -> Dict[str, float]:
    """Return cached ACE multipliers, reloading from Sheets every few minutes."""
    if not ACE_ENABLED:
        return {k: 1.0 for k in VOICE_KEYS}

    now = time.time()
    with _cache_lock:
        if now - float(_cache["ts"]) > ACE_CACHE_SECONDS:
            _refresh_cache_locked(now)
        return dict(_cache["mult"])  # shallow copy

def apply_ace_bias(council: Dict[str, float]) -> Dict[str, float]:
    """
    Apply ACE multipliers to a council weight dict.

    Input: {"soul": 1.0, "nova": 0.8, ...}
    Output: biased + renormalized dict (max weight = 1.0).
    """
    if not ACE_ENABLED:
        return council

    try:
        mult = get_ace_multipliers()
    except Exception as e:
        _log(f"ACE multiplier fetch failed: {e}")
        return council

    # Multiply
    biased: Dict[str, float] = {}
    for k, v in council.items():
        key = str(k).strip().lower()
        try:
            m = float(mult.get(key, 1.0))
            biased[k] = float(v or 0.0) * m
        except Exception:
            biased[k] = float(v) if v not in (None, "") else 0.0

    # Renormalize so the strongest voice is 1.0 (preserves your original behavior)
    max_val = max(biased.values() or [0.0])
    if max_val <= 0:
        return council

    for k in list(biased.keys()):
        biased[k] = round(float(biased[k]) / max_val, 3)

    return biased
