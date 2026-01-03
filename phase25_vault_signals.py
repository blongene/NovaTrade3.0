"""
phase25_vault_signals.py — Phase 25A/B helpers (Bus)

Purpose
- Compute "Vault Intelligence" and "Rebuy/Rotation" signals WITHOUT writing to Sheets.
- Designed for Phase 25 observation: decision records should be human-readable and low-noise.

Inputs (read-only; DB-first when available)
- Vaults (or VAULT_SRC_TAB): current positions with ROI/Status fields
- Vault_ROI_Tracker (optional): historical ROI snapshots
- Vault Intelligence (optional): tag memory + rebuy_ready
- Unified_Snapshot (optional): quote budgets (via venue_budget)

Outputs
- A list of signal dicts, each:
  {
    "type": "SELL_CANDIDATE" | "REBUY_CANDIDATE" | "WATCH",
    "token": "MIND",
    "confidence": 0.0-1.0,
    "reasons": [...],
    "facts": {...}   # lightweight numbers
  }

Safety
- No Sheet writes.
- Any failure -> returns [] and an error string, never raises.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional, Tuple

from utils import get_records_cached, str_or_empty, safe_float  # type: ignore


def _tab(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v or default


VAULT_TAB = _tab("VAULT_SRC_TAB", "Vaults")
VAULT_INTEL_TAB = os.getenv("VAULT_INTELLIGENCE_WS", "Vault Intelligence")
VAULT_ROI_TAB = os.getenv("VAULT_ROI_TAB", "Vault_ROI_Tracker")


def _read_records_prefer_db(tab: str) -> List[Dict[str, Any]]:
    # DB-first adapter if present, otherwise Sheets cached.
    try:
        from db_read_adapter import get_records_prefer_db  # type: ignore

        rows = get_records_prefer_db(
            tab,
            f"sheet_mirror:{tab}",
            sheets_fallback_fn=lambda *args, **kwargs: get_records_cached(tab),
        )
        return rows or []
    except Exception:
        try:
            return get_records_cached(tab) or []
        except Exception:
            return []


def _latest_roi_by_token(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    From Vault_ROI_Tracker: find latest ROI per token.
    Expected columns commonly: Timestamp, Token, ROI, USD Value, Status
    """
    out: Dict[str, float] = {}
    # iterate newest-last; keep first seen per token
    for r in reversed(rows or []):
        tok = str_or_empty(r.get("Token") or r.get("token")).upper()
        if not tok or tok in out:
            continue
        roi = safe_float(r.get("ROI") or r.get("roi"))
        if roi is None:
            continue
        out[tok] = float(roi)
    return out


def compute_vault_signals(max_items: int = 25) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Returns (signals, error). error is None if ok.
    """
    try:
        vault_rows = _read_records_prefer_db(VAULT_TAB)
        intel_rows = _read_records_prefer_db(VAULT_INTEL_TAB)
        roi_rows = _read_records_prefer_db(VAULT_ROI_TAB)

        latest_roi = _latest_roi_by_token(roi_rows)

        # Build quick maps
        rebuy_ready: Dict[str, bool] = {}
        memory_tags: Dict[str, str] = {}
        for r in intel_rows:
            tok = str_or_empty(r.get("Token") or r.get("token")).upper()
            if not tok:
                continue
            rr = str_or_empty(r.get("rebuy_ready") or r.get("Rebuy Ready") or r.get("Rebuy")).upper()
            if rr in ("TRUE", "YES", "1", "Y"):
                rebuy_ready[tok] = True
            tag = str_or_empty(r.get("Memory Tag") or r.get("memory_tag") or r.get("Tag"))
            if tag:
                memory_tags[tok] = tag

        signals: List[Dict[str, Any]] = []

        # Thresholds (safe defaults; can override with env)
        sell_roi_floor = float(os.getenv("PHASE25_SELL_ROI_FLOOR", "-15"))  # sell if ROI <= -15%
        watch_roi_floor = float(os.getenv("PHASE25_WATCH_ROI_FLOOR", "-7"))  # watch if ROI <= -7%
        min_usd_to_care = float(os.getenv("PHASE25_MIN_POSITION_USD", "10"))  # ignore dust

        for r in vault_rows:
            tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Asset")).upper()
            if not tok:
                continue

            usd_val = safe_float(r.get("USD Value") or r.get("usd_value") or r.get("Value_USD") or r.get("Value"))
            if usd_val is not None and float(usd_val) < min_usd_to_care:
                continue

            roi = safe_float(r.get("ROI") or r.get("roi"))
            if roi is None:
                roi = latest_roi.get(tok)

            status = str_or_empty(r.get("Status") or r.get("status")).upper()
            rotation_candidate = str_or_empty(r.get("RotationCandidate") or r.get("Rotation Candidate") or r.get("Rotate")).upper()
            rotate_flag = rotation_candidate in ("TRUE", "YES", "1", "Y")

            reasons: List[str] = []
            facts: Dict[str, Any] = {}
            if usd_val is not None:
                facts["usd_value"] = float(usd_val)
            if roi is not None:
                facts["roi"] = float(roi)
            if status:
                facts["status"] = status
            if tok in memory_tags:
                facts["memory_tag"] = memory_tags[tok]

            # SELL candidate rules
            if rotate_flag:
                reasons.append("rotation_candidate=TRUE")
            if roi is not None and float(roi) <= sell_roi_floor:
                reasons.append(f"roi<= {sell_roi_floor}%")
            if status in ("STALL", "STALLED", "DELIST", "RISK"):
                reasons.append(f"status={status}")

            if reasons:
                # confidence: simple heuristic
                conf = 0.55
                if rotate_flag:
                    conf += 0.15
                if roi is not None and float(roi) <= sell_roi_floor:
                    conf += 0.20
                if status in ("DELIST", "RISK"):
                    conf += 0.10
                conf = max(0.0, min(1.0, conf))

                signals.append({
                    "type": "SELL_CANDIDATE",
                    "token": tok,
                    "confidence": conf,
                    "reasons": reasons,
                    "facts": facts,
                })
                continue

            # WATCH rules
            if roi is not None and float(roi) <= watch_roi_floor:
                signals.append({
                    "type": "WATCH",
                    "token": tok,
                    "confidence": 0.40,
                    "reasons": [f"roi<= {watch_roi_floor}%"],
                    "facts": facts,
                })

            # REBUY candidate only if explicitly tagged
            if rebuy_ready.get(tok):
                signals.append({
                    "type": "REBUY_CANDIDATE",
                    "token": tok,
                    "confidence": 0.55,
                    "reasons": ["rebuy_ready=TRUE"],
                    "facts": facts,
                })

            if len(signals) >= max_items:
                break

        # Rank: SELL first, then REBUY, then WATCH
        order = {"SELL_CANDIDATE": 0, "REBUY_CANDIDATE": 1, "WATCH": 2}
        signals.sort(key=lambda s: (order.get(s.get("type"), 9), -float(s.get("confidence") or 0.0)))

        return signals[:max_items], None
    except Exception as e:
        return [], f"{e.__class__.__name__}:{e}"
