#!/usr/bin/env python3
"""
telemetry_mirror.py — NovaTrade 3.0

Purpose:
    Read the latest Edge → Bus telemetry snapshot (_last_tel from wsgi)
    and append a compact balance snapshot into Wallet_Monitor.

    This keeps Wallet_Monitor fresh using the same telemetry source the
    rest of the system sees, without putting Sheets in the hot path.

Expected telemetry shape (as used elsewhere in NovaTrade 3.0):

    _last_tel = {
        "agent": "edge-primary,edge-nl1",
        "by_venue": {
            "COINBASE":  {"USDC": 136.38, "USDT": 0.0,    ...},
            "BINANCEUS": {"USD": 9.77,   "USDC": 0.0,
                          "USDT": 159.25, ...},
            "KRAKEN":    {"USDC": 0.00005, "USDT": 155.78, ...},
        },
        "flat": {
            "BTC": 0.015,
            "ETH": 0.42,
            ...
        },
        "ts": 1700000000,
    }
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List

import json
import os
import time

import requests

from utils import (
    sheets_append_rows,
    get_ws_cached,
    warn,
    info,
)  # type: ignore

# Base URL to talk to our own Bus. PORT is present in Render.
_PORT = os.getenv("PORT", "10000")
_BASE = os.getenv("TELEMETRY_LAST_URL_BASE", f"http://localhost:{_PORT}")
LAST_URL = os.getenv("TELEMETRY_LAST_URL", f"{_BASE}/api/telemetry/last")

SHEET_URL = os.getenv("SHEET_URL", "")
WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")
WALLET_MONITOR_MAX_ROWS = int(os.getenv("WALLET_MONITOR_MAX_ROWS", "500"))

# Ignore dust balances below this threshold
TELEMETRY_MIRROR_MIN_BALANCE = float(
    os.getenv("TELEMETRY_MIRROR_MIN_BALANCE", "0.0000001")
)

# Simple stables list so we can tag Quote column
STABLES = {"USD", "USDT", "USDC"}

# Compaction: keep at most this many data rows (excluding header).
# 0 or negative disables compaction.
WALLET_MONITOR_MAX_ROWS = int(os.getenv("WALLET_MONITOR_MAX_ROWS", "1000"))


def _http_get_last() -> Dict[str, Any]:
    """
    Call the local /api/telemetry/last endpoint exposed by wsgi.py and
    return the inner telemetry dict (same shape as _last_tel).

    The endpoint usually returns:
        {"ok": true, "data": {...}, "age_sec": 7.3}
    """
    try:
        resp = requests.get(LAST_URL, timeout=5)
    except Exception as e:
        warn(f"telemetry_mirror: error calling {LAST_URL}: {e}")
        return {}

    if resp.status_code != 200:
        warn(f"telemetry_mirror: {LAST_URL} -> HTTP {resp.status_code}")
        return {}

    try:
        body = resp.json()
    except Exception as e:
        warn(f"telemetry_mirror: invalid JSON from {LAST_URL}: {e}")
        return {}

    if not body.get("ok"):
        warn(f"telemetry_mirror: endpoint returned ok=false: {body}")
        return {}

    data = body.get("data") or {}
    if not isinstance(data, dict):
        return {}
    return data


def _summarize_by_venue(by_venue: Dict[str, Any]) -> str:
    """
    Build a compact human-readable snapshot of balances for logs.
    Example:
        'COINBASE:USDC=136.38; BINANCEUS:USD=9.77,USDT=159.25; ...'
    """
    parts: List[str] = []
    for venue, assets in sorted(by_venue.items()):
        if not isinstance(assets, dict):
            continue
        frag_parts: List[str] = []
        for asset, qty in sorted(assets.items()):
            try:
                qf = float(qty or 0.0)
            except Exception:
                continue
            if qf <= TELEMETRY_MIRROR_MIN_BALANCE:
                continue
            frag_parts.append(f"{asset}={qf:g}")
        if frag_parts:
            parts.append(f"{venue}:" + ",".join(frag_parts))
    return "; ".join(parts)


def _compact_wallet_monitor_if_needed() -> None:
    """
    Bounded-history compaction for Wallet_Monitor.

    Strategy (per-agent, but sheet-wide):
      - Look at column A (Timestamp) to find the last non-empty row.
      - Keep the header + the most recent WALLET_MONITOR_MAX_ROWS rows.
      - Delete ONLY full, contiguous blocks of old rows (2..N).
    """
    if WALLET_MONITOR_MAX_ROWS <= 0:
        # 0 or negative = compaction disabled
        return

    if not SHEET_URL:
        warn("[telemetry_mirror] compaction skipped: SHEET_URL not set")
        return

    # 1) Open the sheet
    try:
        ws = get_ws_cached(SHEET_URL, WALLET_MONITOR_WS)
    except Exception as e:
        warn(
            f"[telemetry_mirror] compaction skipped: "
            f"cannot open Wallet_Monitor ({WALLET_MONITOR_WS}): {e!r}"
        )
        return

    # 2) Read column A (timestamps)
    try:
        col_a = ws.col_values(1)  # 1-based, includes header
    except Exception as e:
        warn(
            f"[telemetry_mirror] compaction failed reading col A in "
            f"{WALLET_MONITOR_WS}: {e!r}"
        )
        return

    if len(col_a) <= 1:
        # Only header (or totally empty)
        return

    last_data_row = len(col_a)         # last non-empty row index (1-based)
    data_rows = last_data_row - 1      # excluding header row

    if data_rows <= WALLET_MONITOR_MAX_ROWS:
        # Already within bounds
        return

    # 3) Compute the range of old rows to delete
    delete_count = data_rows - WALLET_MONITOR_MAX_ROWS

    # We always keep row 1 (header) and the last N data rows
    start_row = 2                      # first data row
    end_row = 1 + delete_count         # inclusive, gspread uses 1-based

    try:
        ws.delete_rows(start_row, end_row)
        info(
            "[telemetry_mirror] compacted Wallet_Monitor: "
            f"removed {delete_count} old rows (kept last {WALLET_MONITOR_MAX_ROWS})"
        )
    except Exception as e:
        warn(
            f"[telemetry_mirror] compaction delete_rows({start_row}, {end_row}) "
            f"failed on {WALLET_MONITOR_WS}: {e!r}"
        )

def mirror_telemetry_once() -> None:
    """
    Pull the latest telemetry snapshot and mirror balances into Wallet_Monitor,
    then compact the tab if it exceeds WALLET_MONITOR_MAX_ROWS.
    """
    if not SHEET_URL:
        warn("telemetry_mirror: SHEET_URL missing; abort.")
        return

    data = _http_get_last()
    if not data:
        return

    by_venue = data.get("by_venue") or {}
    if not isinstance(by_venue, dict):
        warn("telemetry_mirror: telemetry has no by_venue; nothing to mirror.")
        return

    ts = data.get("ts") or time.time()
    # Accept either seconds or ms; if ts is large, assume ms
    if isinstance(ts, (int, float)) and ts > 10_000_000_000:
        ts = ts / 1000.0
    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    now_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    info(
        f"telemetry_mirror: using snapshot agent={data.get('agent')} "
        f"age={time.time() - float(ts):.0f}s venues={len(by_venue)} "
        f"raw={_summarize_by_venue(by_venue)}"
    )

    rows: List[List[Any]] = []

    for venue, assets in by_venue.items():
        if not isinstance(assets, dict):
            continue
        venue_u = str(venue).upper()
        for asset, qty in assets.items():
            try:
                qf = float(qty or 0.0)
            except Exception:
                continue
            if qf <= TELEMETRY_MIRROR_MIN_BALANCE:
                continue
            asset_u = str(asset).upper()
            quote = asset_u if asset_u in STABLES else ""
            # Wallet_Monitor columns:
            #   Timestamp, Venue, Asset, Free, Locked, Quote
            rows.append([now_str, venue_u, asset_u, qf, 0.0, quote])

    if not rows:
        info("telemetry_mirror: no non-zero balances to mirror; nothing to do.")
        return

    # Append new rows
    sheets_append_rows(SHEET_URL, WALLET_MONITOR_WS, rows)
    info(
        f"telemetry_mirror: mirrored {len(rows)} balances into {WALLET_MONITOR_WS}."
    )

    # Compact historical rows if needed
    _compact_wallet_monitor_if_needed()


def main() -> None:
    mirror_telemetry_once()


if __name__ == "__main__":
    main()
