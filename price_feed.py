"""
price_feed.py

B-2 price feed for NovaTrade.

Reads Unified_Snapshot and exposes:

    get_price_usd(token: str, quote: str = "USDT", venue: str | None = None) -> float | None

Strategy:
  • Prefer rows matching (venue, token, quote) if venue is provided.
  • Otherwise, fall back to the freshest row for (token, quote) across all venues.
  • If no explicit price column exists, attempt to derive from Equity_USD / Total.
"""

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
        ws = sh.worksheet(unified_snapshot_ws := UNIFIED_SNAPSHOT_WS)
        rows = ws.get_all_records()
    except Exception as e:
        warn(f"price_feed: failed to load {UNIFIED_SNAPSHOT_WS}: {e}")
        rows = []

    _snapshot_cache["rows"] = rows or []
    _snapshot_cache["ts"] = now
    return _snapshot_cache["rows"]


def _extract_price_from_row(row: dict) -> Optional[float]:
    """Try several common column names to derive a USD price."""
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

    # Fallback: Equity_USD / Total
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


def _row_matches(row: dict, token: str, quote: str) -> bool:
    asset = str(row.get("Asset") or row.get("Token") or "").upper()
    quote_sym = str(row.get("QuoteSymbol") or row.get("Quote") or "").upper()
    if not asset:
        return False
    if asset != token:
        return False
    if quote and quote_sym and quote_sym != quote:
        return False
    return True


def _parse_ts(row: dict) -> float:
    """Parse Timestamp column into a float; fall back to 0 if not parseable."""
    ts_val = row.get("Timestamp") or row.get("TS") or ""
    if isinstance(ts_val, (int, float)):
        return float(ts_val)
    if isinstance(ts_val, str) and ts_val.strip():
        try:
            # Try ISO8601
            from datetime import datetime

            return datetime.fromisoformat(ts_val.replace("Z", "+00:00")).timestamp()
        except Exception:
            # Try epoch-like
            try:
                return float(ts_val)
            except Exception:
                return 0.0
    return 0.0


def get_price_usd(token: str, quote: str = "USDT", venue: Optional[str] = None) -> Optional[float]:
    """
    Return a USD price for (token, quote, venue).

    Preference:
      1) rows in Unified_Snapshot matching (venue, token, quote) with the freshest timestamp.
      2) otherwise, freshest row for (token, quote) across all venues.
      3) if no quote is provided, match token only and use any quote's price.
    """
    token = (token or "").upper()
    quote = (quote or "").upper()
    venue = (venue or "").upper() or None

    if not token:
        return None

    rows = _load_snapshot_rows(force=False)
    candidates: List[Dict[str, Any]] = []

    for r in rows:
        if not _row_matches(r, token, quote):
            continue

        v = str(r.get("Venue") or "").upper()
        price = _extract_price_from_row(r)
        if price is None or price <= 0:
            continue

        candidates.append(
            {
                "venue": v,
                "price": price,
                "ts": _parse_ts(r),
            }
        )

    if not candidates:
        return None

    # If venue specified, prefer freshest candidate for that venue
    if venue:
        v_matches = [c for c in candidates if c["venue"] == venue]
        if v_matches:
            best = max(v_matches, key=lambda c: c["ts"])
            return float(best["price"])

    # Fallback: freshest candidate overall
    best = max(candidates, key=lambda c: c["ts"])
    return float(best["price"])
