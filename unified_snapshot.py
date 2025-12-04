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
from typing import Dict, List, Tuple, Any

from utils import get_ws, warn, info

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


def run_unified_snapshot() -> None:
    info("📸 unified_snapshot: building Unified_Snapshot from Wallet_Monitor…")

    rows = _load_wallet_rows()
    if not rows:
        print("unified_snapshot: no Wallet_Monitor rows; aborting snapshot.")
        return

    latest = _latest_per_venue_asset(rows)
    if not latest:
        print("unified_snapshot: no usable (venue, asset) pairs; aborting snapshot.")
        return

    ts_now = int(time.time())
    ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_now))

    out_rows: List[List[Any]] = []

    # 3×3 stable grid (venues × stables)
    for venue in VENUES:
        for asset in STABLES:
            key = (venue, asset)
            vals = latest.get(key, {"free": 0.0, "locked": 0.0})
            free = vals["free"]
            locked = vals["locked"]
            total = free + locked
            is_quote = True
            quote_sym = asset
            equity_usd = total  # treat stables as 1:1 USD

            out_rows.append(
                [
                    ts_str,
                    venue,
                    asset,
                    free,
                    locked,
                    total,
                    is_quote,
                    quote_sym,
                    equity_usd,
                ]
            )

    # (Optional) You can extend here to add alt-coins below the 3×3 grid later.

    try:
        ws = get_ws(UNIFIED_SNAPSHOT_WS)
    except Exception as e:
        warn(f"unified_snapshot: cannot open Unified_Snapshot: {e}")
        return

    try:
        ws.clear()
        ws.append_row(US_HEADER, value_input_option="USER_ENTERED")
        try:
            from utils import sheets_append_rows

            # FIX: pass rows as a keyword so the helper sees it
            sheets_append_rows(ws, rows=out_rows)
        except Exception:
            for r in out_rows:
                ws.append_row(r, value_input_option="USER_ENTERED")
    
    if len(latest) < 3:
        warn(f"unified_snapshot: too few venue/asset keys ({len(latest)}); aborting snapshot for safety.")
        return

    for v in VENUES:
        for a in STABLES:
            if (v, a) not in latest:
                warn(f"unified_snapshot: missing ({v}, {a}) pair; aborting.")
                return
 
    except Exception as e:
        warn(f"unified_snapshot: write failed: {e}")
        return

    info(f"✅ unified_snapshot: wrote {len(out_rows)} rows to {UNIFIED_SNAPSHOT_WS}")
    print(f"unified_snapshot: wrote {len(out_rows)} rows to {UNIFIED_SNAPSHOT_WS}")


if __name__ == "__main__":
    run_unified_snapshot()
