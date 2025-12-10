#!/usr/bin/env python3
"""
ace_bias.py
Phase 21 – ACE (Autonomous Council Engine) bias helpers.

Reads the Council_Performance sheet and produces per-voice
weight multipliers used to gently bias council weights.

Safe by design:
- If anything fails (env, Sheets, auth), it falls back to 1.0 for all voices.
- Only affects the "council" metadata on decisions, not actual trade policy.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Dict

VOICE_KEYS = ("soul", "nova", "orion", "ash", "lumen", "vigil")

ACE_ENABLED = os.getenv("COUNCIL_ACE_ENABLE", "1").lower() in ("1", "true", "yes", "on")
SHEET_URL = os.getenv("SHEET_URL")
ACE_WS = os.getenv("COUNCIL_ACE_WS", "Council_Performance")
ACE_CACHE_SECONDS = int(os.getenv("COUNCIL_ACE_CACHE_SECONDS", "300"))

_cache_lock = threading.Lock()
_cache: Dict[str, object] = {
    "ts": 0.0,
    "mult": {k: 1.0 for k in VOICE_KEYS},
}


def _log(msg: str) -> None:
    try:
        print(f"[ace_bias] {msg}")
    except Exception:
        pass


def _open_worksheet():
    """Open the Council_Performance worksheet via service account."""
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

    # svc may be a path or a raw JSON string
    if svc.strip().startswith("{"):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(svc), scope
        )
    else:
        creds = ServiceAccountCredentials.from_json_keyfile_name(svc, scope)

    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet(ACE_WS)


def _refresh_cache_locked(now: float) -> None:
    """Reload ACE multipliers from Council_Performance into the cache."""
    if not SHEET_URL:
        _log("SHEET_URL not set; ACE disabled.")
        return

    try:
        ws = _open_worksheet()
        values = ws.get_all_values()
        if not values:
            return

        headers = values[0]
        rows = values[1:]  # data rows

        def idx(name: str) -> int:
            try:
                return headers.index(name)
            except ValueError:
                return -1

        col_voice = idx("Voice")
        col_mult = idx("ACE_Weight_Multiplier")

        if col_voice < 0 or col_mult < 0:
            _log("Missing Voice or ACE_Weight_Multiplier columns; ACE disabled.")
            return

        mult: Dict[str, float] = {k: 1.0 for k in VOICE_KEYS}

        for row in rows:
            if not row or len(row) <= max(col_voice, col_mult):
                continue
            name = (row[col_voice] or "").strip().lower()
            if not name:
                continue
            try:
                val = float(row[col_mult])
            except Exception:
                continue

            key = name.lower()
            if key in mult:
                # Keep within a reasonable range
                mult[key] = max(0.25, min(2.0, val))

        _cache["ts"] = now
        _cache["mult"] = mult
        _log(f"Refreshed ACE multipliers: {mult}")
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

    Input: {"soul": 1.0, "nova": 1.0, ...}
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
        try:
            m = float(mult.get(k, 1.0))
            biased[k] = float(v) * m
        except Exception:
            biased[k] = float(v) or 0.0

    # Renormalize so the strongest voice is 1.0
    max_val = max(biased.values() or [0.0])
    if max_val <= 0:
        return council

    for k in list(biased.keys()):
        biased[k] = round(biased[k] / max_val, 3)

    return biased
