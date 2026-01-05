"""
phase25_vault_signals.py — Phase 25A/B helpers (Bus)

Purpose
- Compute "Vault Intelligence" and "Rebuy/Rotation" signals WITHOUT writing to Sheets.
- Designed for Phase 25 observation: decision records should be human-readable and low-noise.

Inputs (read-only; DB-first when available)
- Vaults (or VAULT_SRC_TAB): current positions with ROI/Status fields (best case)
- Vault_ROI_Tracker (optional): historical ROI snapshots (fallback for ROI + USD value)
- Vault Intelligence (optional): tag memory + rebuy_ready (enables REBUY candidates)
- Unified_Snapshot (optional): venue budgets / balances (fallback for quotes)

Outputs
- A list of signal dicts, each:
  {
    "type": "SELL_CANDIDATE" | "REBUY_CANDIDATE" | "WATCH",
    "token": "BTC",
    "confidence": 0.0-1.0,
    "reasons": [...],
    "facts": {...}   # lightweight numbers
  }

Safety
- No Sheet writes.
- Any failure -> returns [] and an error string, never raises.

Notes (Phase 25)
- If VAULT_TAB is empty / missing, we still attempt to produce signals from ROI + Intel tabs.
- We intentionally avoid creating "new buy ideas" unless explicitly tagged (rebuy_ready) OR
  existing holdings show risk / rotation signals.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from utils import get_records_cached, str_or_empty, safe_float  # type: ignore


def _tab(name: str, default: str) -> str:
    v = (os.getenv(name, "") or "").strip()
    return v or default


# Primary sources (defaults are safe; can override with env)
VAULT_TAB = _tab("VAULT_SRC_TAB", "Vaults")
VAULT_INTEL_TAB = _tab("VAULT_INTELLIGENCE_WS", "Vault Intelligence")
VAULT_ROI_TAB = _tab("VAULT_ROI_TAB", "Vault_ROI_Tracker")
UNIFIED_SNAPSHOT_TAB = _tab("UNIFIED_SNAPSHOT_TAB", "Unified_Snapshot")


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


def _latest_by_token(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    From a time-series tab (e.g., Vault_ROI_Tracker), return latest row per token.
    We iterate newest-last and keep the first row seen for each token.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for r in reversed(rows or []):
        tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Asset")).upper()
        if not tok or tok in out:
            continue
        out[tok] = r
    return out


def _parse_rebuy_ready(intel_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, bool], Dict[str, str]]:
    rebuy_ready: Dict[str, bool] = {}
    memory_tags: Dict[str, str] = {}
    for r in intel_rows or []:
        tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Asset")).upper()
        if not tok:
            continue
        rr = str_or_empty(r.get("rebuy_ready") or r.get("Rebuy Ready") or r.get("Rebuy") or r.get("RebuyReady")).upper()
        if rr in ("TRUE", "YES", "1", "Y"):
            rebuy_ready[tok] = True
        tag = str_or_empty(r.get("Memory Tag") or r.get("memory_tag") or r.get("Tag") or r.get("Memory"))
        if tag:
            memory_tags[tok] = tag
    return rebuy_ready, memory_tags


def _quote_budgets_from_unified(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Best-effort quote budgets. Unified_Snapshot varies; we look for rows Class=QUOTE.
    Returns total USD-ish across venues (still approximate).
    """
    total = 0.0
    for r in rows or []:
        cls = str_or_empty(r.get("Class") or r.get("class")).upper()
        if cls != "QUOTE":
            continue
        free = safe_float(r.get("Free") or r.get("free") or r.get("Amount") or r.get("Balance"))
        if free is None:
            continue
        total += float(free)
    return {"total_quote": round(total, 6)}


def compute_vault_signals(max_items: int = 25) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Returns (signals, error). error is None if ok.
    Designed to be resilient: missing tabs simply reduce signal richness.
    """
    try:
        vault_rows = _read_records_prefer_db(VAULT_TAB)
        intel_rows = _read_records_prefer_db(VAULT_INTEL_TAB)
        roi_rows = _read_records_prefer_db(VAULT_ROI_TAB)
        unified_rows = _read_records_prefer_db(UNIFIED_SNAPSHOT_TAB)

        rebuy_ready, memory_tags = _parse_rebuy_ready(intel_rows)
        latest_roi_row = _latest_by_token(roi_rows)
        quote_budget = _quote_budgets_from_unified(unified_rows)

        signals: List[Dict[str, Any]] = []

        # Thresholds (safe defaults; override with env)
        sell_roi_floor = float(os.getenv("PHASE25_SELL_ROI_FLOOR", "-15"))   # sell if ROI <= -15%
        watch_roi_floor = float(os.getenv("PHASE25_WATCH_ROI_FLOOR", "-7"))  # watch if ROI <= -7%
        min_usd_to_care = float(os.getenv("PHASE25_MIN_POSITION_USD", "10")) # ignore dust
        rebuy_min_quote = float(os.getenv("PHASE25_REBUY_MIN_QUOTE", "25"))  # don't even suggest rebuy if quote is tiny

        # Helper: build facts from the best info we have
        def _facts_for(tok: str, vault_row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            facts: Dict[str, Any] = {}
            if tok in memory_tags:
                facts["memory_tag"] = memory_tags[tok]
            if vault_row:
                usd_val = safe_float(vault_row.get("USD Value") or vault_row.get("usd_value") or vault_row.get("Value_USD") or vault_row.get("Value"))
                roi = safe_float(vault_row.get("ROI") or vault_row.get("roi"))
                status = str_or_empty(vault_row.get("Status") or vault_row.get("status")).upper()
                if usd_val is not None:
                    facts["usd_value"] = float(usd_val)
                if roi is not None:
                    facts["roi"] = float(roi)
                if status:
                    facts["status"] = status
                rc = str_or_empty(vault_row.get("RotationCandidate") or vault_row.get("Rotation Candidate") or vault_row.get("Rotate")).upper()
                if rc:
                    facts["rotation_candidate"] = rc
            else:
                # ROI fallback
                rr = latest_roi_row.get(tok) or {}
                roi = safe_float(rr.get("ROI") or rr.get("roi"))
                usd_val = safe_float(rr.get("USD Value") or rr.get("usd_value") or rr.get("Value_USD") or rr.get("Value"))
                status = str_or_empty(rr.get("Status") or rr.get("status")).upper()
                if roi is not None:
                    facts["roi"] = float(roi)
                if usd_val is not None:
                    facts["usd_value"] = float(usd_val)
                if status:
                    facts["status"] = status

            # include quote context lightly (helpful for rebuy reasoning)
            if quote_budget:
                facts.update(quote_budget)
            return facts

        # --- Primary path: Vaults tab provides holdings + flags ---
        for r in vault_rows or []:
            tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Asset")).upper()
            if not tok:
                continue

            usd_val = safe_float(r.get("USD Value") or r.get("usd_value") or r.get("Value_USD") or r.get("Value"))
            if usd_val is not None and float(usd_val) < min_usd_to_care:
                continue

            roi = safe_float(r.get("ROI") or r.get("roi"))
            if roi is None:
                # fallback ROI from tracker
                roi = safe_float((latest_roi_row.get(tok) or {}).get("ROI") or (latest_roi_row.get(tok) or {}).get("roi"))

            status = str_or_empty(r.get("Status") or r.get("status")).upper()
            rotation_candidate = str_or_empty(r.get("RotationCandidate") or r.get("Rotation Candidate") or r.get("Rotate")).upper()
            rotate_flag = rotation_candidate in ("TRUE", "YES", "1", "Y")

            reasons: List[str] = []
            facts = _facts_for(tok, r)

            # SELL candidate rules (only when we have a concrete reason)
            if rotate_flag:
                reasons.append("rotation_candidate=TRUE")
            if roi is not None and float(roi) <= sell_roi_floor:
                reasons.append(f"roi<= {sell_roi_floor}%")
            if status in ("STALL", "STALLED", "DELIST", "RISK"):
                reasons.append(f"status={status}")

            if reasons:
                conf = 0.55
                if rotate_flag:
                    conf += 0.15
                if roi is not None and float(roi) <= sell_roi_floor:
                    conf += 0.20
                if status in ("DELIST", "RISK"):
                    conf += 0.10
                conf = max(0.0, min(1.0, conf))

                signals.append({"type": "SELL_CANDIDATE", "token": tok, "confidence": conf, "reasons": reasons, "facts": facts})
                continue

            # WATCH: negative ROI but not full sell threshold
            if roi is not None and float(roi) <= watch_roi_floor:
                signals.append({"type": "WATCH", "token": tok, "confidence": 0.40, "reasons": [f"roi<= {watch_roi_floor}%"], "facts": facts})

            # REBUY candidates: ONLY if explicitly tagged
            if rebuy_ready.get(tok):
                # optional budget guard (avoid constant suggestions when no quote)
                tq = safe_float(facts.get("total_quote"))
                if tq is None or float(tq) >= rebuy_min_quote:
                    signals.append({"type": "REBUY_CANDIDATE", "token": tok, "confidence": 0.55, "reasons": ["rebuy_ready=TRUE"], "facts": facts})
                else:
                    signals.append({"type": "WATCH", "token": tok, "confidence": 0.20, "reasons": [f"rebuy_ready=TRUE but total_quote<{rebuy_min_quote}"], "facts": facts})

            if len(signals) >= max_items:
                break

        # --- Fallback path: if Vaults is empty, build from ROI + Intel only ---
        if not vault_rows:
            # SELL/WATCH from ROI tracker if any
            for tok, rr in (latest_roi_row or {}).items():
                if not tok:
                    continue
                facts = _facts_for(tok, None)
                usd_val = safe_float(facts.get("usd_value"))
                if usd_val is not None and float(usd_val) < min_usd_to_care:
                    continue

                roi = safe_float(facts.get("roi"))
                status = str_or_empty(facts.get("status")).upper()

                reasons: List[str] = []
                if roi is not None and float(roi) <= sell_roi_floor:
                    reasons.append(f"roi<= {sell_roi_floor}%")
                if status in ("STALL", "STALLED", "DELIST", "RISK"):
                    reasons.append(f"status={status}")

                if reasons:
                    conf = 0.60
                    if roi is not None and float(roi) <= sell_roi_floor:
                        conf += 0.20
                    if status in ("DELIST", "RISK"):
                        conf += 0.10
                    conf = max(0.0, min(1.0, conf))
                    signals.append({"type": "SELL_CANDIDATE", "token": tok, "confidence": conf, "reasons": reasons, "facts": facts})
                elif roi is not None and float(roi) <= watch_roi_floor:
                    signals.append({"type": "WATCH", "token": tok, "confidence": 0.35, "reasons": [f"roi<= {watch_roi_floor}%"], "facts": facts})

                if len(signals) >= max_items:
                    break

            # REBUY from Intel tags even if token not in vault rows
            for tok, rr in (rebuy_ready or {}).items():
                if not rr:
                    continue
                facts = _facts_for(tok, None)
                tq = safe_float(facts.get("total_quote"))
                if tq is None or float(tq) >= rebuy_min_quote:
                    signals.append({"type": "REBUY_CANDIDATE", "token": tok, "confidence": 0.50, "reasons": ["rebuy_ready=TRUE (intel)"], "facts": facts})
                else:
                    signals.append({"type": "WATCH", "token": tok, "confidence": 0.20, "reasons": [f"rebuy_ready=TRUE but total_quote<{rebuy_min_quote}"], "facts": facts})

        # Rank: SELL first, then REBUY, then WATCH
        order = {"SELL_CANDIDATE": 0, "REBUY_CANDIDATE": 1, "WATCH": 2}
        signals.sort(key=lambda s: (order.get(str(s.get("type")), 9), -float(s.get("confidence") or 0.0)))

        return signals[:max_items], None
    except Exception as e:
        return [], f"{e.__class__.__name__}:{e}"
