#!/usr/bin/env python3
"""
unified_snapshot.py — NovaTrade 3.0

Phase 19: build Unified_Snapshot from Wallet_Monitor in a way that is
robust to schema drift and bad data.

Design goals
------------
* Never crash on weird sheet contents (strings like "QUOTE" where numbers
  are expected, partially-filled rows, etc.).
* Work with both of these Wallet_Monitor schemas:

    Legacy:
        Timestamp | Venue | Asset | Free | Locked | Quote | Snapshot

    Telemetry v2 (from telemetry_mirror.py):
        Timestamp | Agent | Venue | Asset | Amount | Class | Snapshot

* Always produce exactly 9 rows in Unified_Snapshot:
    (COINBASE, BINANCEUS, KRAKEN) × (USD, USDC, USDT)

The Unified_Snapshot sheet has the header:

    Timestamp | Venue | Asset | Free | Locked | Total | IsQuote | QuoteSymbol | Equity_USD
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple

from utils import with_sheet_backoff
from utils import get_ws_cached, info, warn


# ---- Config ----

UNIFIED_SNAPSHOT_SRC_WS = os.getenv("UNIFIED_SNAPSHOT_SRC_WS", "Wallet_Monitor")
UNIFIED_SNAPSHOT_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")

VENUES: List[str] = ["COINBASE", "BINANCEUS", "KRAKEN"]
STABLES: List[str] = ["USD", "USDC", "USDT"]

HEADER: List[str] = [
    "Timestamp",
    "Venue",
    "Asset",
    "Free",
    "Locked",
    "Total",
    "IsQuote",
    "QuoteSymbol",
    "Equity_USD",
]


# ---- Sheet helpers (with backoff) ----


@with_sheet_backoff
def _open_ws(name: str):
    """Open a worksheet by name using the cached Sheets helper."""
    return get_ws_cached(name)


@with_sheet_backoff
def _replace_rows(ws, header: List[str], rows: List[List[Any]]) -> None:
    """
    Replace all data in `ws` with the provided header and rows.

    We intentionally bypass any helper around sheets_append_rows because
    its signature has changed over time. This keeps the function
    self-contained and stable.
    """
    # Clear existing content, then write header + rows.
    ws.clear()
    all_rows: List[List[Any]] = [header] + rows
    ws.append_rows(all_rows, value_input_option="RAW")


# ---- Utility helpers ----


def _safe_float(v: Any) -> float:
    """Best-effort numeric coercion. Non-numeric values become 0.0."""
    if v is None:
        return 0.0
    try:
        s = str(v).strip()
        if not s:
            return 0.0
        # tolerate commas in thousands separators
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return 0.0


def _parse_ts(ts_raw: Any, idx_fallback: int) -> float:
    """
    Parse a timestamp string into a float epoch.

    If parsing fails, fall back to the row index so that later rows win.
    """
    if not ts_raw:
        return float(idx_fallback)

    ts_str = str(ts_raw).strip()
    # Common formats we see in Sheets.
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(ts_str.replace("Z", ""), fmt).timestamp()
        except Exception:
            pass

    return float(idx_fallback)


# ---- Core logic ----


def _latest_wallet_records(
    rows: Iterable[Dict[str, Any]]
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    From raw Wallet_Monitor rows, compute the *latest* record per (venue, asset).

    We support both schemas by probing for Free/Locked first and falling
    back to Amount when those are empty.
    """
    latest: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for idx, row in enumerate(rows):
        venue = (row.get("Venue") or "").strip().upper()
        asset = (row.get("Asset") or "").strip().upper()

        if not venue or not asset:
            continue
        if venue not in VENUES or asset not in STABLES:
            continue

        ts_raw = row.get("Timestamp") or ""
        ts_val = _parse_ts(ts_raw, idx)

        # Prefer explicit Free/Locked; if both are zero, fall back to Amount.
        free = _safe_float(row.get("Free"))
        locked = _safe_float(row.get("Locked"))
        amount = _safe_float(row.get("Amount"))

        if free == 0.0 and locked == 0.0 and amount != 0.0:
            free = amount
            locked = 0.0

        key = (venue, asset)
        prev = latest.get(key)
        if prev is not None and prev["ts"] >= ts_val:
            # We already have a newer record for this (venue, asset).
            continue

        latest[key] = {
            "ts": ts_val,
            "Timestamp": str(ts_raw) if ts_raw else "",
            "Venue": venue,
            "Asset": asset,
            "Free": free,
            "Locked": locked,
        }

    return latest


def _build_unified_rows(latest: Dict[Tuple[str, str], Dict[str, Any]]) -> List[List[Any]]:
    """
    Turn the latest-per-(venue, asset) dict into the 9 Unified_Snapshot rows.

    If no data exists for a (venue, asset) combination, we still emit a
    row with zeros so callers always see 9 rows.
    """
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    out: List[List[Any]] = []

    for venue in VENUES:
        for asset in STABLES:
            key = (venue, asset)
            rec = latest.get(key)

            if rec:
                ts = rec.get("Timestamp") or now_str
                free = float(rec.get("Free") or 0.0)
                locked = float(rec.get("Locked") or 0.0)
            else:
                ts = now_str
                free = 0.0
                locked = 0.0

            total = free + locked
            is_quote = True  # these are all stables
            quote_symbol = asset
            equity_usd = total  # 1:1 with USD-denom stables

            out.append(
                [
                    ts,
                    venue,
                    asset,
                    free,
                    locked,
                    total,
                    is_quote,
                    quote_symbol,
                    equity_usd,
                ]
            )

    return out


def run_unified_snapshot() -> None:
    """Public entrypoint used both by the scheduler and manual runs."""
    info("📸 unified_snapshot: building Unified_Snapshot from Wallet_Monitor…")

    try:
        src_ws = _open_ws(UNIFIED_SNAPSHOT_SRC_WS)
    except Exception as e:
        warn(
            f"unified_snapshot: failed to open source worksheet "
            f"'{UNIFIED_SNAPSHOT_SRC_WS}': {e}"
        )
        return

    try:
        rows = src_ws.get_all_records()
    except Exception as e:
        warn(
            f"unified_snapshot: get_all_records() failed for "
            f"'{UNIFIED_SNAPSHOT_SRC_WS}': {e}"
        )
        return

    info(f"unified_snapshot: Wallet_Monitor get_all_records -> {len(rows)} rows")
    if rows:
        sample = rows[min(0, len(rows) - 1)]
        info(f"unified_snapshot: sample row: {sample}")

    latest = _latest_wallet_records(rows)
    info(f"unified_snapshot: latest per (venue, asset) -> {len(latest)} keys")

    out_rows = _build_unified_rows(latest)

    try:
        out_ws = _open_ws(UNIFIED_SNAPSHOT_WS)
    except Exception as e:
        warn(
            f"unified_snapshot: failed to open Unified_Snapshot worksheet "
            f"'{UNIFIED_SNAPSHOT_WS}': {e}"
        )
        return

    try:
        _replace_rows(out_ws, HEADER, out_rows)
        info(f"✅ unified_snapshot: wrote {len(out_rows)} rows to Unified_Snapshot")
    except Exception as e:
        warn(f"unified_snapshot: failed writing rows to Unified_Snapshot: {e}")


if __name__ == "__main__":
    run_unified_snapshot()
