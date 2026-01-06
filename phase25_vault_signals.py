"""
phase25_vault_signals.py — Phase 25A/B helpers (Bus)

Purpose
- Compute Vault + Rebuy signals WITHOUT writing to Sheets.
- Phase 25-safe: low-noise, tolerant of missing tabs, idempotent outputs.

Key upgrades (Jan 2026)
1) Quote normalization: compute a GLOBAL stable-quote budget (USD/USDC/USDT/DAI) using Wallet_Monitor snapshot text.
2) Signal memory: keep lightweight in-process counts/streaks (exposed to decision records as "memory").
3) Alpha namespace (prep-only): optional ALPHA_WATCH signals from TrendTracker/Listings when enabled.

Safety
- No Sheet writes.
- Any failure -> returns [] + error string (never raises).
- Alpha is OFF by default and cannot influence Vault execution unless explicitly enabled via DB_READ_JSON.

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

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from utils import get_records_cached, str_or_empty, safe_float  # type: ignore


# -----------------------
# Tabs / config helpers
# -----------------------

def _tab(env_name: str, default: str) -> str:
    v = os.getenv(env_name, "").strip()
    return v or default


VAULT_TAB = _tab("VAULT_SRC_TAB", "Token_Vault")
VAULT_INTEL_TAB = _tab("VAULT_INTEL_TAB", "Vault_Intelligence")
VAULT_ROI_TAB = _tab("VAULT_ROI_TAB", "Vault_ROI_Tracker")
WALLET_MONITOR_TAB = _tab("WALLET_MONITOR_TAB", "Wallet_Monitor")

ALPHA_TREND_TAB = _tab("ALPHA_TREND_TAB", "TrendTracker")
ALPHA_LISTINGS_TAB = _tab("ALPHA_LISTINGS_TAB", "Listings")


def _read_db_read_json() -> Dict[str, Any]:
    """
    Best-effort parse of DB_READ_JSON env var.
    If missing/invalid -> {}.
    """
    raw = os.getenv("DB_READ_JSON", "").strip()
    if not raw:
        return {}
    try:
        import json
        return json.loads(raw) if isinstance(raw, str) else {}
    except Exception:
        return {}


def _phase25_cfg() -> Dict[str, Any]:
    return (_read_db_read_json() or {}).get("phase25") or {}


def _alpha_cfg() -> Dict[str, Any]:
    # alpha config is nested under phase25.alpha, default disabled
    p = _phase25_cfg()
    return p.get("alpha") or {}


def _alpha_enabled() -> bool:
    try:
        return bool(int(_alpha_cfg().get("enabled", 0)))
    except Exception:
        return False


def _min_total_quote_usd() -> float:
    """
    Conservative rebuy floor. Defaults to 25 USD if not specified.
    Override via DB_READ_JSON.phase25.min_total_quote_usd or env MIN_TOTAL_QUOTE_USD.
    """
    env_v = os.getenv("MIN_TOTAL_QUOTE_USD", "").strip()
    if env_v:
        try:
            return float(env_v)
        except Exception:
            pass
    try:
        return float(_phase25_cfg().get("min_total_quote_usd", 25.0))
    except Exception:
        return 25.0


# -----------------------
# DB-first reads (safe)
# -----------------------

def _read_records_prefer_db(tab: str) -> List[Dict[str, Any]]:
    """
    DB-first adapter if present, otherwise Sheets cached.
    Never raises; returns [] on failure.
    """
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
# ROI helpers
# -----------------------

def _latest_roi_by_token(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Extract latest ROI by token from Vault_ROI_Tracker-like rows.
    Tolerant to schema drift.
    """
    latest: Dict[str, Tuple[float, float]] = {}  # token -> (ts_epoch, roi)
    for r in rows:
        tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Symbol") or r.get("symbol")).upper()
        if not tok:
            continue
        roi = safe_float(r.get("Vault ROI") or r.get("Vault_ROI") or r.get("roi") or r.get("ROI"))
        if roi is None:
            continue
        ts = r.get("Date") or r.get("date") or r.get("Timestamp") or r.get("timestamp") or ""
        ts_epoch = _safe_ts_epoch(ts)
        cur = latest.get(tok)
        if cur is None or ts_epoch >= cur[0]:
            latest[tok] = (ts_epoch, float(roi))
    return {k: v[1] for k, v in latest.items()}


def _safe_ts_epoch(ts: Any) -> float:
    if not ts:
        return 0.0
    s = str(ts).strip()
    # very permissive; if parse fails, treat as 0
    try:
        # YYYY-MM-DD HH:MM:SS
        import datetime
        if len(s) >= 19 and s[4] == "-" and s[7] == "-":
            dt = datetime.datetime.fromisoformat(s.replace("Z", ""))
            return dt.timestamp()
    except Exception:
        pass
    return 0.0


# -----------------------
# Quote aggregation (GLOBAL)
# -----------------------

_STABLE_QUOTES = {"USD", "USDC", "USDT", "DAI", "TUSD", "FDUSD"}


def _extract_snapshot_text(row: Dict[str, Any]) -> str:
    return str_or_empty(
        row.get("Snapshot")
        or row.get("snapshot")
        or row.get("Balances")
        or row.get("balances")
        or ""
    )


def _parse_compact_snapshot_total(snapshot: str) -> float:
    """
    Parses strings like:
      "BINANCEUS:USD=9.77,USDT=159.253; COINBASE:USD=8.01117,USDC=41.6153,USDT=58.3081"
    Returns summed stable-quote total across venues.
    """
    if not snapshot:
        return 0.0
    total = 0.0
    # Find ASSET=NUMBER pairs
    for asset, num in re.findall(r"([A-Za-z]{2,10})\s*=\s*([0-9]+(?:\.[0-9]+)?)", snapshot):
        a = asset.strip().upper()
        if a in _STABLE_QUOTES:
            try:
                total += float(num)
            except Exception:
                continue
    return float(total)


def _compute_total_quote_from_wallet_monitor(rows: List[Dict[str, Any]]) -> float:
    """
    Prefer parsing the compact snapshot string from the most recent Wallet_Monitor row.
    Fallback: sum Free across recent rows with Class=QUOTE and stable assets.
    """
    if not rows:
        return 0.0

    # 1) Prefer snapshot parsing from newest non-empty snapshot
    for r in reversed(rows[-25:]):
        snap = _extract_snapshot_text(r)
        if snap:
            val = _parse_compact_snapshot_total(snap)
            if val > 0:
                return val

    # 2) Fallback: sum free stable quotes in recent rows
    total = 0.0
    for r in rows[-200:]:
        cls = str_or_empty(r.get("Class") or r.get("class")).upper()
        asset = str_or_empty(r.get("Asset") or r.get("asset")).upper()
        if cls != "QUOTE":
            continue
        if asset not in _STABLE_QUOTES:
            continue
        f = safe_float(r.get("Free") or r.get("free"))
        if f is None:
            continue
        total += float(f)
    return float(total)


def get_total_quote_usd() -> float:
    """
    Public helper: best-effort global quote total.
    """
    rows = _read_records_prefer_db(WALLET_MONITOR_TAB)
    return _compute_total_quote_from_wallet_monitor(rows)


# -----------------------
# Signal memory (in-process)
# -----------------------

# NOTE: This is intentionally in-process only (no DB/Sheet writes) for Phase 25 safety.
# It persists across cycles as long as the Bus process stays up (Render redeploy resets it).

_SIGNAL_MEM: Dict[str, Any] = {
    "watch": {},   # token -> {count, first_ts, last_ts, streak}
    "rebuy": {},
    "sell": {},
    "alpha": {},
    "last_seen": {},  # token -> ts
}


def _touch_bucket(bucket: str, token: str, ts: str) -> None:
    b = _SIGNAL_MEM.setdefault(bucket, {})
    rec = b.get(token) or {"count": 0, "first_ts": ts, "last_ts": ts, "streak": 0}
    rec["count"] = int(rec.get("count", 0)) + 1
    # streak increments if last_seen was also within recent window (very simple)
    last_seen = str_or_empty(_SIGNAL_MEM.get("last_seen", {}).get(token))
    if last_seen:
        rec["streak"] = int(rec.get("streak", 0)) + 1
    else:
        rec["streak"] = 1
    rec["last_ts"] = ts
    if not rec.get("first_ts"):
        rec["first_ts"] = ts
    b[token] = rec
    _SIGNAL_MEM.setdefault("last_seen", {})[token] = ts


def update_signal_memory(signals: List[Dict[str, Any]], ts: str) -> Dict[str, Any]:
    """
    Update in-process memory based on current signals.
    Returns a compact snapshot suitable for embedding in decision records.
    """
    # Clear last_seen each cycle (streak is per "consecutive sightings")
    _SIGNAL_MEM["last_seen"] = {}

    for s in signals or []:
        typ = str_or_empty(s.get("type")).upper()
        tok = str_or_empty(s.get("token")).upper()
        if not tok or not typ:
            continue
        if typ == "WATCH":
            _touch_bucket("watch", tok, ts)
        elif typ == "REBUY_CANDIDATE":
            _touch_bucket("rebuy", tok, ts)
        elif typ == "SELL_CANDIDATE":
            _touch_bucket("sell", tok, ts)
        elif typ == "ALPHA_WATCH":
            _touch_bucket("alpha", tok, ts)

    # Return a compact summary (top tokens by count)
    def top(bucket: str, n: int = 5) -> List[Dict[str, Any]]:
        b = _SIGNAL_MEM.get(bucket) or {}
        items = []
        for tok, rec in b.items():
            items.append({"token": tok, **rec})
        items.sort(key=lambda x: (-int(x.get("count", 0)), x.get("token", "")))
        return items[:n]

    return {
        "watch_top": top("watch", 8),
        "rebuy_top": top("rebuy", 8),
        "sell_top": top("sell", 8),
        "alpha_top": top("alpha", 8),
    }


def get_signal_memory_snapshot() -> Dict[str, Any]:
    """
    Read-only snapshot (does not mutate).
    """
    return {
        "watch": _SIGNAL_MEM.get("watch") or {},
        "rebuy": _SIGNAL_MEM.get("rebuy") or {},
        "sell": _SIGNAL_MEM.get("sell") or {},
        "alpha": _SIGNAL_MEM.get("alpha") or {},
    }


# -----------------------
# Alpha signals (prep-only)
# -----------------------

def _first_matching_key(row: Dict[str, Any], candidates: List[str]) -> Optional[str]:
    keys = list(row.keys())
    low = {str(k).strip().lower(): k for k in keys}
    for c in candidates:
        k = low.get(c.lower())
        if k is not None:
            return k
    # fuzzy: contains
    for c in candidates:
        for lk, orig in low.items():
            if c.lower() in lk:
                return orig
    return None


def _compute_alpha_watch_signals(max_items: int = 10) -> List[Dict[str, Any]]:
    """
    Optional, low-confidence watchlist from Alpha tabs.
    OFF by default.
    """
    trend_rows = _read_records_prefer_db(ALPHA_TREND_TAB)
    list_rows = _read_records_prefer_db(ALPHA_LISTINGS_TAB)

    out: List[Dict[str, Any]] = []

    # TrendTracker: Token + Interest (Past 7 Days)
    for r in trend_rows[-200:]:
        tok_key = _first_matching_key(r, ["Token", "token", "Symbol", "symbol"])
        if not tok_key:
            continue
        tok = str_or_empty(r.get(tok_key)).upper()
        if not tok:
            continue
        interest_key = _first_matching_key(r, ["Interest (Past 7 Days)", "interest", "interest_7d"])
        interest = safe_float(r.get(interest_key)) if interest_key else None
        if interest is None:
            continue
        if float(interest) < 10.0:  # conservative default filter
            continue
        out.append({
            "type": "ALPHA_WATCH",
            "token": tok,
            "confidence": 0.15,
            "reasons": ["trend_spike"],
            "facts": {"interest_7d": float(interest)},
        })
        if len(out) >= max_items:
            break

    # Listings: Symbol + Status
    for r in list_rows[-200:]:
        sym_key = _first_matching_key(r, ["Symbol", "symbol", "Ticker", "ticker"])
        if not sym_key:
            continue
        tok = str_or_empty(r.get(sym_key)).upper()
        if not tok:
            continue
        status_key = _first_matching_key(r, ["Status", "status"])
        status = str_or_empty(r.get(status_key)) if status_key else ""
        if status and status.upper() not in ("NEW", "WATCH", "TRACK", "LISTED"):
            continue
        out.append({
            "type": "ALPHA_WATCH",
            "token": tok,
            "confidence": 0.10,
            "reasons": ["new_listing_watch"],
            "facts": {"status": status or "unknown"},
        })
        if len(out) >= max_items:
            break

    # Deduplicate by token (keep highest confidence)
    best: Dict[str, Dict[str, Any]] = {}
    for s in out:
        tok = str_or_empty(s.get("token")).upper()
        if not tok:
            continue
        cur = best.get(tok)
        if cur is None or float(s.get("confidence") or 0.0) > float(cur.get("confidence") or 0.0):
            best[tok] = s
    dedup = list(best.values())
    dedup.sort(key=lambda s: -float(s.get("confidence") or 0.0))
    return dedup[:max_items]


# -----------------------
# Vault signals
# -----------------------

def compute_vault_signals(max_items: int = 25) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Returns (signals, error). error is None if ok.
    """
    try:
        vault_rows = _read_records_prefer_db(VAULT_TAB)
        intel_rows = _read_records_prefer_db(VAULT_INTEL_TAB)
        roi_rows = _read_records_prefer_db(VAULT_ROI_TAB)

        latest_roi = _latest_roi_by_token(roi_rows)

        # Build quick maps from Vault_Intelligence
        rebuy_ready: Dict[str, bool] = {}
        memory_tags: Dict[str, str] = {}
        for r in intel_rows:
            tok = str_or_empty(r.get("Token") or r.get("token")).upper()
            if not tok:
                continue
            rr = str_or_empty(r.get("rebuy_ready") or r.get("Rebuy Ready") or r.get("Rebuy_Ready")).strip().upper()
            rebuy_ready[tok] = rr in ("TRUE", "YES", "1", "Y")
            mt = str_or_empty(r.get("memory_tag") or r.get("Memory Tag") or r.get("Memory_Tag"))
            if mt:
                memory_tags[tok] = mt

        # Global quote budget (used to safely gate rebuys)
        total_quote = get_total_quote_usd()
        min_quote = _min_total_quote_usd()

        # Thresholds (conservative defaults)
        sell_roi_floor = float(os.getenv("SELL_ROI_FLOOR", "-12"))   # <= -12% -> sell candidate
        watch_roi_floor = float(os.getenv("WATCH_ROI_FLOOR", "-6"))  # <= -6% -> watch
        min_usd_to_care = float(os.getenv("MIN_USD_TO_CARE", "5"))   # ignore dust

        signals: List[Dict[str, Any]] = []

        for r in vault_rows:
            tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Symbol") or r.get("symbol")).upper()
            if not tok:
                continue

            usd_val = safe_float(r.get("USD Value") or r.get("usd_value") or r.get("Value_USD") or r.get("Value"))
            if usd_val is not None and float(usd_val) < min_usd_to_care:
                continue

            roi = safe_float(r.get("ROI") or r.get("roi"))
            if roi is None:
                roi = latest_roi.get(tok)

            status = str_or_empty(r.get("Status") or r.get("status")).upper()
            rotate_flag = str_or_empty(r.get("Rotation Candidate") or r.get("Rotate") or r.get("RotationCan...ate")).upper() in ("TRUE", "YES", "1", "Y")

            facts: Dict[str, Any] = {"total_quote": float(total_quote)}
            if usd_val is not None:
                facts["usd_value"] = float(usd_val)
            if roi is not None:
                facts["roi"] = float(roi)
            if status:
                facts["status"] = status
            if tok in memory_tags:
                facts["memory_tag"] = memory_tags[tok]

            # SELL candidate rules
            if rotate_flag or (roi is not None and float(roi) <= sell_roi_floor):
                reasons: List[str] = []
                if rotate_flag:
                    reasons.append("rotation_candidate=TRUE")
                if roi is not None and float(roi) <= sell_roi_floor:
                    reasons.append(f"roi<= {sell_roi_floor}%")
                signals.append({
                    "type": "SELL_CANDIDATE",
                    "token": tok,
                    "confidence": 0.65,
                    "reasons": reasons or ["policy_sell_floor"],
                    "facts": facts,
                })

            # WATCH on poor ROI
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
                # Gate to WATCH if we do not have sufficient global quote
                if float(total_quote) < float(min_quote):
                    signals.append({
                        "type": "WATCH",
                        "token": tok,
                        "confidence": 0.20,
                        "reasons": [f"rebuy_ready=TRUE but total_quote<{float(min_quote):.1f}"],
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

        # Optional Alpha watch signals (prep-only)
        if _alpha_enabled():
            try:
                alpha = _compute_alpha_watch_signals(max_items=10)
                signals.extend(alpha)
            except Exception:
                # never fail vault signals due to alpha
                pass

        # Rank: SELL first, then REBUY, then WATCH, then ALPHA_WATCH
        order = {"SELL_CANDIDATE": 0, "REBUY_CANDIDATE": 1, "WATCH": 2, "ALPHA_WATCH": 3}
        signals.sort(key=lambda s: (order.get(str_or_empty(s.get("type")).upper(), 9), -float(s.get("confidence") or 0.0)))

        return signals[:max_items], None
    except Exception as e:
        return [], f"{e.__class__.__name__}:{e}"
