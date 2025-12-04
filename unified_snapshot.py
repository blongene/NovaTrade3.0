#!/usr/bin/env python3
"""
unified_snapshot.py — NovaTrade 3.0 (Phase 19)

Builds a stable-coin Unified_Snapshot from Wallet_Monitor.

Expected Wallet_Monitor header (what we see in Sheets today):

    Timestamp | Venue | Asset | Free | Locked | Quote | Snapshot

Unified_Snapshot header:

    Timestamp | Venue | Asset | Free | Locked | Total | IsQuote | QuoteSymbol | Equity_USD

We produce a 3 × 3 grid:

    Venues: COINBASE, BINANCEUS, KRAKEN
    Assets: USD, USDC, USDT

and fill in the latest balances from Wallet_Monitor.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Dict, List, Tuple, Any

from utils import get_ws, warn, info
from utils import get_ws_cached, with_sheet_backoff

UNIFIED_SNAPSHOT_SRC_WS = os.getenv("UNIFIED_SNAPSHOT_SRC_WS", "Wallet_Monitor")
UNIFIED_SNAPSHOT_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")
WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")

VENUES = ["COINBASE", "BINANCEUS", "KRAKEN"]
STABLES = ["USD", "USDC", "USDT"]

US_HEADER = [
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


def _safe_float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0


def _load_wallet_rows() -> List[Dict[str, Any]]:
    """Return all Wallet_Monitor rows as dicts."""
    try:
        ws = get_ws(WALLET_MONITOR_WS)
    except Exception as e:
        warn(f"unified_snapshot: cannot open Wallet_Monitor: {e}")
        return []

    try:
        rows = ws.get_all_records()
    except Exception as e:
        warn(f"unified_snapshot: get_all_records failed: {e}")
        return []

    print(f"unified_snapshot: Wallet_Monitor get_all_records -> {len(rows)} rows")
    if rows:
        print("unified_snapshot: sample row:", rows[0])
    return rows


def _latest_per_venue_asset(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, float]]:
    """
    From Wallet_Monitor rows, keep only the newest per (venue, asset),
    based on row order (Sheet is append-only, newest last).
    """
    latest: Dict[Tuple[str, str], Dict[str, float]] = {}

    # iterate backwards so the first time we see a key is the newest row
    for r in reversed(rows):
        venue = str(r.get("Venue") or "").upper()
        asset = str(r.get("Asset") or "").upper()
        if not venue or not asset:
            continue

        key = (venue, asset)
        if key in latest:
            continue  # already captured the newest

        free = _safe_float(r.get("Free"))
        locked = _safe_float(r.get("Locked"))
        latest[key] = {"free": free, "locked": locked}

    print(f"unified_snapshot: latest per (venue, asset) -> {len(latest)} keys")
    return latest


def build_snapshot(rows: List[Dict[str, str]]) -> List[List[Any]]:
    """
    Convert Wallet_Monitor rows into the 9-row Unified_Snapshot format.

    Output rows:
        Timestamp, Venue, Asset, Free, Locked, Total, IsQuote, QuoteSymbol, Equity_USD
    """
    # (venue, asset) → latest row
    latest: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for row in rows:
        venue = (row.get("Venue") or "").upper()
        asset = (row.get("Asset") or "").upper()
        ts_raw = row.get("Timestamp") or ""

        if not venue or not asset:
            continue

        # Use timestamp as tie-breaker
        try:
            ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", ""))
            except Exception:
                continue

        key = (venue, asset)
        prev = latest.get(key)
        if prev:
            try:
                prev_ts = datetime.strptime(prev["Timestamp"], "%Y-%m-%d %H:%M:%S")
            except Exception:
                prev_ts = ts  # fallback but won't override
            if prev_ts >= ts:
                continue

        latest[key] = {
            "Timestamp": ts_raw,
            "Venue": venue,
            "Asset": asset,
            "Free": row.get("Free") or 0,
            "Locked": row.get("Locked") or 0,
            "Quote": row.get("Class") == "QUOTE",
        }

    # Now format rows for Unified_Snapshot
    out = []
    for (venue, asset), rec in sorted(latest.items()):
        free = float(rec["Free"] or 0)
        locked = float(rec["Locked"] or 0)
        total = free + locked
        is_quote = rec["Quote"]
        quote_symbol = asset if is_quote else ""
        equity = total if is_quote else ""

        out.append(
            [
                rec["Timestamp"],
                venue,
                asset,
                free,
                locked,
                total,
                is_quote,
                quote_symbol,
                equity,
            ]
        )

    return out

@with_sheet_backoff
def _open_ws(name: str):
    """Open a worksheet with cache + backoff."""
    return get_ws_cached(name)


@with_sheet_backoff
def _replace_unified_rows(ws, rows: List[List[Any]]):
    """
    Replace all data rows in Unified_Snapshot while keeping the header.

    - Keeps row 1 (header) if present.
    - Shrinks the sheet to 1 row, then appends new data.
    """
    # Try to keep existing header; if missing, create a default one.
    try:
        header = ws.row_values(1)
    except Exception:
        header = []

    if not header:
        header = [
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

    # Keep just the header row
    try:
        ws.resize(rows=1)
    except Exception:
        # If resize fails, we still attempt to overwrite from row 2 down
        pass

    # Ensure header row is written
    ws.update("A1", [header])

    # Append new data rows
    if rows:
        ws.append_rows(rows, value_input_option="RAW")

def run_unified_snapshot() -> None:
    """Standalone executable for cron/scheduler."""
    info("📊 unified_snapshot: building Unified_Snapshot from Wallet_Monitor…")

    ws = get_ws_cached(UNIFIED_SNAPSHOT_SRC_WS)
    rows = ws.get_all_records()

    out = build_snapshot(rows)
    if not out:
        warn("unified_snapshot: no usable (venue, asset) pairs; aborting snapshot.")
        return

    # Write output
    out_ws = _open_unified_ws()
    _replace_unified_rows(out_ws, out)
    
    info(f"unified_snapshot: wrote {len(out)} rows to Unified_Snapshot")

if __name__ == "__main__":
    run_unified_snapshot()
