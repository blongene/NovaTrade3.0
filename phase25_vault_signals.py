"""
phase25_vault_signals.py — Phase 25A/B helpers (Bus)

Purpose
- Compute Vault + Rebuy signals WITHOUT writing to Sheets.
- Phase 25-safe: low-noise, tolerant of missing tabs, idempotent outputs.

Key upgrades (Jan 2026)
1) Quote normalization: compute a GLOBAL stable-quote budget (USD/USDC/USDT/DAI)
   using Wallet_Monitor snapshot compact text (supports scientific notation like 1e-06).
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
    "facts": {...}   # lightweight numbers
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


# Prefer canonical names, tolerate legacy where useful.
VAULT_TAB = _tab("VAULT_SRC_TAB", "Token_Vault")

VAULT_INTEL_TAB = _tab("VAULT_INTEL_TAB", "Vault_Intelligence")
# Some older sheets used a spaced tab name; accept both.
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
    """
    Alpha is OFF by default.
    Enable only via DB_READ_JSON:
      - {"alpha": {"enabled": 1}}
      - or {"phase25": {"alpha_enabled": 1}}
    """
    cfg = _load_db_read_json()

    alpha = cfg.get("alpha") or {}
    if isinstance(alpha, dict):
        v = str(alpha.get("enabled", "0")).strip().lower()
        if v in ("1", "true", "yes", "y"):
            return True

    p25 = cfg.get("phase25") or {}
    if isinstance(p25, dict):
        v = str(p25.get("alpha_enabled", "0")).strip().lower()
        if v in ("1", "true", "yes", "y"):
            return True

    return False


# -----------------------
# DB-first read helper
# -----------------------

def _read_records_prefer_db(tab: str) -> List[Dict[str, Any]]:
    """
    DB-first adapter if present, otherwise Sheets cached.

    IMPORTANT: do NOT assume get_records_cached supports ttl_s (historic gotcha).
    """
    try:
        from db_read_adapter import get_records_prefer_db  # type: ignore

        # Some adapters accept extra args. We pass cache_key + sheets fallback explicitly.
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
# Snapshot parsing (global quote)
# -----------------------

# Allows decimal + scientific notation (e.g., 1e-06), with optional sign.
_PAIR_RE = re.compile(r"\b([A-Z0-9]{2,10})=([0-9eE+\-\.]+)\b")


def _quote_breakdown_from_snapshot(snapshot: str) -> Dict[str, float]:
    """
    Return per-stable currency totals from a compact snapshot string.

    Example snapshot:
      "BINANCEUS:USD=9.77,USDT=159.253; COINBASE:USD=8.01,USDC=36.6,USDT=58.3; ..."
    """
    if not snapshot:
        return {}
    stables = {"USD", "USDC", "USDT", "DAI"}
    out: Dict[str, float] = {}
    for sym, val in _PAIR_RE.findall(snapshot.upper()):
        if sym not in stables:
            continue
        amt = safe_float(val)
        if amt is None:
            continue
        out[sym] = float(out.get(sym, 0.0)) + float(amt)
    # NaN guard
    for k, v in list(out.items()):
        if v != v:
            out[k] = 0.0
    return out


def _total_quote_from_breakdown(breakdown: Dict[str, float]) -> float:
    if not breakdown:
        return 0.0
    total = float(sum(float(v) for v in breakdown.values()))
    return 0.0 if (total != total) else float(total)


def _get_quote_facts_from_wallet_monitor() -> Dict[str, Any]:
    """
    Best-effort: derive global quote totals from Wallet_Monitor snapshot.
    Returns:
      {
        "total_quote": float,
        "by_currency": {"USD":..., "USDC":..., ...},
        "snapshot_ts": "YYYY-MM-DD HH:MM:SS" (if available)
      }
    """
    rows = _read_records_prefer_db(WALLET_MONITOR_TAB)
    if not rows:
        return {"total_quote": 0.0, "by_currency": {}}

    # Search last 50 rows for a non-empty snapshot (robust to partial rows)
    for r in reversed(rows[-50:]):
        if not isinstance(r, dict):
            continue
        snap = str_or_empty(r.get("Snapshot") or r.get("snapshot") or r.get("SNAPSHOT"))
        if not snap:
            continue
        breakdown = _quote_breakdown_from_snapshot(snap)
        total = _total_quote_from_breakdown(breakdown)
        out: Dict[str, Any] = {"total_quote": total, "by_currency": breakdown}
        ts = str_or_empty(r.get("Timestamp") or r.get("timestamp") or r.get("ts") or r.get("TS"))
        if ts:
            out["snapshot_ts"] = ts
        return out

    return {"total_quote": 0.0, "by_currency": {}}


# -----------------------
# Signal memory (in-process)
# -----------------------

_MEM: Dict[str, Any] = {
    "since_ts": None,
    "updated_ts": None,
    "counts": {},      # key -> int (e.g., "WATCH:BTC")
    "streaks": {},     # key -> int consecutive buckets
    "last_seen": {},   # key -> unix ts
    "_cycle_key": {},  # key -> last cycle id
}


def _cycle_id() -> int:
    """
    Bucket time to reduce streak flapping. Default aligns to Phase25A interval.
    """
    bucket_s = int(os.getenv("PHASE25_MEMORY_BUCKET_SEC", "1800"))
    return int(int(time.time()) // max(bucket_s, 60))


def _mem_touch(key: str) -> None:
    now = int(time.time())
    if _MEM.get("since_ts") is None:
        _MEM["since_ts"] = now

    cid = _cycle_id()
    counts = _MEM.setdefault("counts", {})
    streaks = _MEM.setdefault("streaks", {})
    last_seen = _MEM.setdefault("last_seen", {})
    cycle_key = _MEM.setdefault("_cycle_key", {})

    counts[key] = int(counts.get(key, 0)) + 1
    last_seen[key] = now

    prev = cycle_key.get(key)
    if prev is not None and int(prev) == cid - 1:
        streaks[key] = int(streaks.get(key, 0)) + 1
    else:
        streaks[key] = 1
    cycle_key[key] = cid

    _MEM["updated_ts"] = now


def update_signal_memory(signals: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update memory from current signals and return a compact snapshot.
    Keys are namespaced as TYPE:TOKEN.
    """
    for s in signals or []:
        if not isinstance(s, dict):
            continue
        typ = str_or_empty(s.get("type")).upper()
        tok = str_or_empty(s.get("token")).upper()
        if not typ or not tok:
            continue
        _mem_touch(f"{typ}:{tok}")

    # Compact top 10 by counts
    counts = dict(sorted((_MEM.get("counts") or {}).items(), key=lambda kv: kv[1], reverse=True)[:10])
    streaks = dict(sorted((_MEM.get("streaks") or {}).items(), key=lambda kv: kv[1], reverse=True)[:10])

    return {
        "since_ts": _MEM.get("since_ts"),
        "updated_ts": _MEM.get("updated_ts"),
        "counts": counts,
        "streaks": streaks,
    }


def get_signal_memory() -> Dict[str, Any]:
    # Do not mutate on read (just return current snapshot)
    counts = dict(_MEM.get("counts") or {})
    streaks = dict(_MEM.get("streaks") or {})
    last_seen = dict(_MEM.get("last_seen") or {})
    return {
        "since_ts": _MEM.get("since_ts"),
        "updated_ts": _MEM.get("updated_ts"),
        "counts": counts,
        "streaks": streaks,
        "last_seen": last_seen,
    }


# -----------------------
# Vault signal logic
# -----------------------

def _latest_roi_by_token(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for r in reversed(rows or []):
        tok = str_or_empty(r.get("Token") or r.get("token")).upper()
        if not tok or tok in out:
            continue
        roi = safe_float(r.get("ROI") or r.get("roi") or r.get("roi_pct") or r.get("Vault ROI") or r.get("Vault_ROI"))
        if roi is None:
            continue
        out[tok] = float(roi)
    return out


def _read_intel_rows() -> List[Dict[str, Any]]:
    rows = _read_records_prefer_db(VAULT_INTEL_TAB)
    if rows:
        return rows
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

    # Listings: Token
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
    """
    Returns (signals, error). error is None if ok.
    """
    try:
        vault_rows = _read_records_prefer_db(VAULT_TAB)
        intel_rows = _read_intel_rows()
        roi_rows = _read_records_prefer_db(VAULT_ROI_TAB)

        quote_facts = _get_quote_facts_from_wallet_monitor()
        total_quote = float(quote_facts.get("total_quote", 0.0) or 0.0)
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
            facts = {"total_quote": total_quote, "min_total_quote": min_total_quote}
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
            signals.extend(_alpha_signals(max_items=max(0, max_items - len(signals))))

        # 3.5) Observation breadcrumb: if no actionable signals yet, emit a single INFO signal
        # derived from the Wallet_Monitor snapshot so we can verify quote aggregation is live.
        if not signals:
            try:
                bcur = quote_facts.get("by_currency") if isinstance(quote_facts, dict) else {}
                bcur = bcur if isinstance(bcur, dict) else {}
                facts = {
                    "total_quote": float(total_quote or 0.0),
                    "by_currency": {k: float(v) for k, v in bcur.items()},
                }
                ts = quote_facts.get("snapshot_ts") if isinstance(quote_facts, dict) else None
                if ts:
                    facts["snapshot_ts"] = str(ts)
                # Only emit if there's something to report (avoid pure noise)
                if facts.get("total_quote", 0.0) > 0.0 or facts.get("by_currency"):
                    signals.append({
                        "type": "INFO",
                        "token": "QUOTE",
                        "confidence": 0.05,
                        "reasons": ["quote_snapshot"],
                        "facts": facts,
                    })
            except Exception:
                pass


        # Rank: SELL first, then REBUY, then WATCH, then ALPHA
        order = {"SELL_CANDIDATE": 0, "REBUY_CANDIDATE": 1, "WATCH": 2, "INFO": 3, "ALPHA_WATCH": 4}
        signals.sort(key=lambda s: (order.get(str(s.get("type") or ""), 9), -float(s.get("confidence") or 0.0)))

        # Update memory (in-process) based on final signals
        update_signal_memory(signals)

        return signals[:max_items], None
    except Exception as e:
        return [], f"{e.__class__.__name__}:{e}"
