"""phase25_vault_signals.py — Phase 25A/B helpers (Bus)

Purpose
- Compute Vault + Rebuy signals WITHOUT writing to Sheets.
- Phase 25-safe: low-noise, tolerant of missing tabs, idempotent outputs.

Key upgrades (Jan 2026)
1) Quote normalization: compute a GLOBAL stable-quote budget (USD/USDC/USDT/DAI)
   using Wallet_Monitor snapshot compact text.
2) Signal memory: keep lightweight in-process counts/streaks (exposed to decision/plan).
3) Alpha namespace (prep-only): optional ALPHA_WATCH signals from TrendTracker/Listings
   when enabled via DB_READ_JSON.

Safety
- No Sheet writes.
- Any failure -> returns [] + error string (never raises).
- Alpha is OFF by default and cannot influence execution unless explicitly enabled.

Expected Outputs
Signal dicts, each:
  {
    "type": "SELL_CANDIDATE" | "REBUY_CANDIDATE" | "WATCH" | "ALPHA_WATCH",
    "token": "BTC",
    "confidence": 0.0-1.0,
    "reasons": [...],
    "facts": {...}
  }
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from utils import get_records_cached, str_or_empty, safe_float  # type: ignore


# -----------------------
# Tabs / config helpers
# -----------------------


def _tab(env_name: str, default: str) -> str:
    v = (os.getenv(env_name) or "").strip()
    return v or default


# Prefer the newer canonical names, but tolerate legacy.
VAULT_TAB = _tab("VAULT_SRC_TAB", "Token_Vault")

# Some older deployments used a spaced name. Accept both.
VAULT_INTEL_TAB = _tab("VAULT_INTEL_TAB", "Vault_Intelligence")
VAULT_INTEL_TAB_LEGACY = _tab("VAULT_INTELLIGENCE_WS", "Vault Intelligence")

VAULT_ROI_TAB = _tab("VAULT_ROI_TAB", "Vault_ROI_Tracker")
WALLET_MONITOR_TAB = _tab("WALLET_MONITOR_TAB", "Wallet_Monitor")

ALPHA_TREND_TAB = _tab("ALPHA_TREND_TAB", "TrendTracker")
ALPHA_LISTINGS_TAB = _tab("ALPHA_LISTINGS_TAB", "Listings")


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _alpha_enabled() -> bool:
    """Alpha is OFF by default. Enable only via DB_READ_JSON."""
    cfg = _load_db_read_json()
    # allow either top-level alpha.enabled or phase25.alpha_enabled
    alpha = cfg.get("alpha") or {}
    if isinstance(alpha, dict) and str(alpha.get("enabled", "0")).strip() in ("1", "true", "TRUE", "yes", "YES"):
        return True
    p25 = cfg.get("phase25") or {}
    if isinstance(p25, dict) and str(p25.get("alpha_enabled", "0")).strip() in ("1", "true", "TRUE", "yes", "YES"):
        return True
    return False


def _read_records_prefer_db(tab: str) -> List[Dict[str, Any]]:
    """DB-first adapter if present, otherwise Sheets cached."""
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


# -----------------------
# Quote parsing (Snapshot)
# -----------------------


_SNAPSHOT_ASSET_RE = re.compile(r"\b([A-Z0-9]{2,10})=([0-9]+(?:\.[0-9]+)?)\b")


def _parse_snapshot_total_quote(snapshot: str) -> float:
    """Parse compact snapshot text and sum stable-quote amounts."""
    if not snapshot:
        return 0.0

    stable = {"USD", "USDC", "USDT", "DAI"}
    total = 0.0
    for m in _SNAPSHOT_ASSET_RE.finditer(snapshot.upper()):
        asset = m.group(1)
        amt = safe_float(m.group(2))
        if amt is None:
            continue
        if asset in stable:
            total += float(amt)
    return float(total)


def _get_total_quote_from_wallet_monitor() -> float:
    """Read latest Wallet_Monitor row and compute total stable quote."""
    rows = _read_records_prefer_db(WALLET_MONITOR_TAB)
    if not rows:
        return 0.0
    last = rows[-1] or {}
    snap = str_or_empty(last.get("Snapshot") or last.get("snapshot") or last.get("SNAPSHOT"))
    return _parse_snapshot_total_quote(snap)


# -----------------------
# Signal memory (in-process)
# -----------------------


_MEM: Dict[str, Any] = {
    "since_ts": None,
    "counts": {},      # token -> int
    "streaks": {},     # token -> int
    "last_seen": {},   # token -> ts
}


def update_signal_memory(signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Update in-process memory from current signal set and return a compact snapshot."""
    now = int(time.time())
    if _MEM.get("since_ts") is None:
        _MEM["since_ts"] = now

    seen_tokens: List[str] = []
    for s in signals or []:
        if not isinstance(s, dict):
            continue
        tok = str_or_empty(s.get("token")).upper()
        if not tok:
            continue
        seen_tokens.append(tok)
        _MEM["counts"][tok] = int(_MEM["counts"].get(tok, 0)) + 1
        _MEM["last_seen"][tok] = now

    # update streaks: increment if seen, else reset to 0
    for tok in list(_MEM["streaks"].keys()):
        if tok not in seen_tokens:
            _MEM["streaks"][tok] = 0
    for tok in seen_tokens:
        _MEM["streaks"][tok] = int(_MEM["streaks"].get(tok, 0)) + 1

    # return compact (top tokens only)
    counts = dict(sorted(_MEM["counts"].items(), key=lambda kv: kv[1], reverse=True)[:10])
    streaks = dict(sorted(_MEM["streaks"].items(), key=lambda kv: kv[1], reverse=True)[:10])
    return {
        "since_ts": _MEM.get("since_ts"),
        "counts": counts,
        "streaks": streaks,
    }


def get_signal_memory() -> Dict[str, Any]:
    return update_signal_memory([])


# -----------------------
# Vault signal logic
# -----------------------


def _latest_roi_by_token(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for r in reversed(rows or []):
        tok = str_or_empty(r.get("Token") or r.get("token")).upper()
        if not tok or tok in out:
            continue
        roi = safe_float(r.get("ROI") or r.get("roi") or r.get("roi_pct"))
        if roi is None:
            continue
        out[tok] = float(roi)
    return out


def _read_intel_rows() -> List[Dict[str, Any]]:
    rows = _read_records_prefer_db(VAULT_INTEL_TAB)
    if rows:
        return rows
    # tolerate legacy spaced tab name
    if VAULT_INTEL_TAB_LEGACY and VAULT_INTEL_TAB_LEGACY != VAULT_INTEL_TAB:
        return _read_records_prefer_db(VAULT_INTEL_TAB_LEGACY)
    return []


def _alpha_signals(max_items: int = 10) -> List[Dict[str, Any]]:
    if not _alpha_enabled():
        return []

    sigs: List[Dict[str, Any]] = []

    # TrendTracker: Token + Score/Momentum
    for r in _read_records_prefer_db(ALPHA_TREND_TAB) or []:
        tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Symbol") or r.get("Asset")).upper()
        if not tok:
            continue
        score = safe_float(r.get("Score") or r.get("score") or r.get("Momentum") or r.get("momentum"))
        if score is None:
            continue
        conf = max(0.05, min(0.6, float(score) / 100.0))
        sigs.append({
            "type": "ALPHA_WATCH",
            "token": tok,
            "confidence": conf,
            "reasons": ["alpha_trend"],
            "facts": {"alpha_score": float(score)},
        })
        if len(sigs) >= max_items:
            break

    # Listings: Token + (optional) listing_date
    if len(sigs) < max_items:
        for r in _read_records_prefer_db(ALPHA_LISTINGS_TAB) or []:
            tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Symbol") or r.get("Asset")).upper()
            if not tok:
                continue
            sigs.append({
                "type": "ALPHA_WATCH",
                "token": tok,
                "confidence": 0.25,
                "reasons": ["new_listing"],
                "facts": {},
            })
            if len(sigs) >= max_items:
                break

    return sigs


def compute_vault_signals(max_items: int = 25) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Returns (signals, error). error is None if ok."""
    try:
        vault_rows = _read_records_prefer_db(VAULT_TAB)
        intel_rows = _read_intel_rows()
        roi_rows = _read_records_prefer_db(VAULT_ROI_TAB)

        total_quote = _get_total_quote_from_wallet_monitor()
        latest_roi = _latest_roi_by_token(roi_rows)

        # Build quick intel maps
        rebuy_ready: Dict[str, bool] = {}
        memory_tags: Dict[str, str] = {}
        for r in intel_rows or []:
            tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Asset")).upper()
            if not tok:
                continue
            rr_raw = r.get("rebuy_ready")
            if rr_raw is None:
                rr_raw = r.get("Rebuy Ready")
            rr = str_or_empty(rr_raw).strip().upper()
            if rr in ("TRUE", "YES", "1", "Y"):
                rebuy_ready[tok] = True
            tag = str_or_empty(r.get("Memory Tag") or r.get("memory_tag") or r.get("Tag"))
            if tag:
                memory_tags[tok] = tag

        signals: List[Dict[str, Any]] = []

        # Thresholds (safe defaults; can override with env)
        sell_roi_floor = float(os.getenv("PHASE25_SELL_ROI_FLOOR", "-15"))
        watch_roi_floor = float(os.getenv("PHASE25_WATCH_ROI_FLOOR", "-7"))
        min_usd_to_care = float(os.getenv("PHASE25_MIN_POSITION_USD", "10"))
        min_total_quote = float(os.getenv("PHASE25_MIN_TOTAL_QUOTE", "25"))

        # 1) Build SELL/WATCH from vault rows when present
        for r in vault_rows or []:
            tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Asset")).upper()
            if not tok:
                continue

            usd_val = safe_float(r.get("USD Value") or r.get("usd_value") or r.get("Value_USD") or r.get("Value"))
            if usd_val is not None and float(usd_val) < min_usd_to_care:
                continue

            roi = safe_float(r.get("ROI") or r.get("roi") or r.get("roi_pct"))
            if roi is None:
                roi = latest_roi.get(tok)

            status = str_or_empty(r.get("Status") or r.get("status")).upper()
            rotation_candidate = str_or_empty(r.get("RotationCandidate") or r.get("Rotation Candidate") or r.get("Rotate")).upper()
            rotate_flag = rotation_candidate in ("TRUE", "YES", "1", "Y")

            facts: Dict[str, Any] = {"total_quote": total_quote}
            if usd_val is not None:
                facts["usd_value"] = float(usd_val)
            if roi is not None:
                facts["roi"] = float(roi)
            if status:
                facts["status"] = status
            if tok in memory_tags:
                facts["memory_tag"] = memory_tags[tok]

            reasons_sell: List[str] = []
            if rotate_flag:
                reasons_sell.append("rotation_candidate=TRUE")
            if roi is not None and float(roi) <= sell_roi_floor:
                reasons_sell.append(f"roi<= {sell_roi_floor}%")
            if status in ("STALL", "STALLED", "DELIST", "RISK"):
                reasons_sell.append(f"status={status}")

            if reasons_sell:
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
                    "reasons": reasons_sell,
                    "facts": facts,
                })
                if len(signals) >= max_items:
                    break
                continue

            # WATCH from ROI
            if roi is not None and float(roi) <= watch_roi_floor:
                signals.append({
                    "type": "WATCH",
                    "token": tok,
                    "confidence": 0.40,
                    "reasons": [f"roi<= {watch_roi_floor}%"],
                    "facts": facts,
                })
                if len(signals) >= max_items:
                    break

        # 2) Rebuy candidates from intel tags (even if vault rows are empty)
        for tok in sorted(rebuy_ready.keys()):
            if any(s.get("token") == tok and s.get("type") in ("SELL_CANDIDATE", "REBUY_CANDIDATE") for s in signals):
                continue
            facts = {"total_quote": total_quote}
            if tok in latest_roi:
                facts["roi"] = float(latest_roi[tok])
            if tok in memory_tags:
                facts["memory_tag"] = memory_tags[tok]

            if total_quote < min_total_quote:
                signals.append({
                    "type": "WATCH",
                    "token": tok,
                    "confidence": 0.20,
                    "reasons": [f"rebuy_ready=TRUE but total_quote<{min_total_quote}"],
                    "facts": facts,
                })
            else:
                signals.append({
                    "type": "REBUY_CANDIDATE",
                    "token": tok,
                    "confidence": 0.55,
                    "reasons": ["rebuy_ready=TRUE"],
                    "facts": facts,
                })

            if len(signals) >= max_items:
                break

        # 3) Optional alpha signals (prep-only)
        if len(signals) < max_items:
            for s in _alpha_signals(max_items=max(0, max_items - len(signals))):
                signals.append(s)

        # Rank: SELL first, then REBUY, then WATCH, then ALPHA
        order = {"SELL_CANDIDATE": 0, "REBUY_CANDIDATE": 1, "WATCH": 2, "ALPHA_WATCH": 3}
        signals.sort(key=lambda s: (order.get(str(s.get("type") or ""), 9), -float(s.get("confidence") or 0.0)))

        # Update memory (in-process) based on final signals
        update_signal_memory(signals)

        return signals[:max_items], None
    except Exception as e:
        return [], f"{e.__class__.__name__}:{e}"
