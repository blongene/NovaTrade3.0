from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from utils import get_gspread_client, warn  # type: ignore

SHEET_URL = os.getenv("SHEET_URL", "").strip()
UNIFIED_SNAPSHOT_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")
PRICE_CACHE_TTL_SEC = int(os.getenv("PRICE_CACHE_TTL_SEC", "60"))

_snapshot_cache: Dict[str, Any] = {
    "ts": 0.0,
    "rows": [],
}


def _load_snapshot_rows(force: bool = False) -> List[dict]:
    """Load Unified_Snapshot rows with a small TTL cache."""
    now = time.time()
    if (
        not force
        and _snapshot_cache.get("rows")
        and now - float(_snapshot_cache.get("ts", 0.0)) < PRICE_CACHE_TTL_SEC
    ):
        return _snapshot_cache["rows"]

    if not SHEET_URL:
        warn("price_feed: SHEET_URL missing.")
        _snapshot_cache["rows"] = []
        _snapshot_cache["ts"] = now
        return []

    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        ws = sh.worksheet(UNIFIED_SNAPSHOT_WS)
        rows = ws.get_all_records()
    except Exception as e:
        warn(f"price_feed: failed to load {UNIFIED_SNAPSHOT_WS}: {e}")
        rows = []

    _snapshot_cache["rows"] = rows or []
    _snapshot_cache["ts"] = now
    return _snapshot_cache["rows"]


def _extract_price_from_row(row: dict) -> Optional[float]:
    """Try to derive a USD price from a Unified_Snapshot row."""
    # 1) Try explicit price-like columns, if they ever appear.
    for key in ("Price_USD", "PriceUsd", "Price", "LastPrice"):
        val = row.get(key)
        if isinstance(val, (int, float)):
            if val > 0:
                return float(val)
        elif isinstance(val, str) and val.strip():
            try:
                f = float(val)
                if f > 0:
                    return f
            except Exception:
                continue

    # 2) Fallback (current NovaTrade design): Equity_USD / Total
    eq = row.get("Equity_USD")
    total = row.get("Total") or row.get("Balance") or row.get("Free")
    try:
        eq_f = float(eq)
        tot_f = float(total)
        if eq_f > 0 and tot_f > 0:
            return eq_f / tot_f
    except Exception:
        pass

    return None


def _parse_ts(row: dict) -> float:
    """Parse Timestamp/TS into epoch seconds, best-effort."""
    ts_val = row.get("Timestamp") or row.get("TS") or ""
    if isinstance(ts_val, (int, float)):
        return float(ts_val)
    if isinstance(ts_val, str) and ts_val.strip():
        from datetime import datetime
        s = ts_val.replace("Z", "").strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(s, fmt).timestamp()
            except Exception:
                continue
        try:
            return float(s)
        except Exception:
            return 0.0
    return 0.0


def get_price_usd(token: str, quote: str = "USDT", venue: Optional[str] = None) -> Optional[float]:
    """
    Return a USD price for `token`.

    Strategy (robust to current Unified_Snapshot layout):

      1. Find rows where Asset == token (case-insensitive).
         We do NOT require QuoteSymbol to match; Unified_Snapshot is one row per
         asset with Equity_USD / Total rather than per trading pair.

      2. If a venue is provided, prefer the freshest row for that venue.

      3. Otherwise, fall back to the freshest candidate overall.

      4. If no suitable row or no usable price can be derived, return None.
    """
    token = (token or "").upper()
    venue = (venue or "").upper() or None

    if not token:
        return None

    rows = _load_snapshot_rows(force=False)
    candidates: List[Dict[str, Any]] = []

    for r in rows:
        asset = str(r.get("Asset") or "").upper()
        if asset != token:
            continue

        price = _extract_price_from_row(r)
        if price is None or price <= 0:
            continue

        cand_venue = str(r.get("Venue") or "").upper()
        candidates.append(
            {
                "venue": cand_venue,
                "price": float(price),
                "ts": _parse_ts(r),
            }
        )

    if not candidates:
        return None

    # Prefer freshest price for the requested venue, if present.
    if venue:
        v_matches = [c for c in candidates if c["venue"] == venue]
        if v_matches:
            best = max(v_matches, key=lambda c: c["ts"])
            return float(best["price"])

    # Fallback: freshest candidate overall.
    best = max(candidates, key=lambda c: c["ts"])
    return float(best["price"])
