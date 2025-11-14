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
        "agent": "edge-primary",
        "by_venue": {
            "COINBASE": {"USDC": 19.30, "BTC": 0.002, ...},
            "BINANCEUS": {"USDT": 17.46, ...},
            ...
        },
        "flat": {...},    # optional per-asset totals
        "ts": 1731600000  # unix timestamp (float)
    }

Writes rows to Wallet_Monitor with columns:
    Timestamp | Venue | Asset | Free | Locked | Quote

- Locked is set to 0.0 (we don't track per-venue lock state here).
- Quote is set to the asset symbol if it's a stablecoin, else "".

Safe to run ad-hoc or on a schedule.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, List

import os
import time
import requests

from utils import sheets_append_rows, warn, info  # type: ignore

# Base URL to talk to our own Bus. PORT is present in Render.
_PORT = os.getenv("PORT", "10000")
_BASE = os.getenv("TELEMETRY_LAST_URL_BASE", f"http://localhost:{_PORT}")
LAST_URL = os.getenv("TELEMETRY_LAST_URL", f"{_BASE}/api/telemetry/last")

SHEET_URL = os.getenv("SHEET_URL", "")

WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")

TELEMETRY_MIRROR_ENABLED = (
    os.getenv("TELEMETRY_MIRROR_ENABLED", "1").lower() in ("1", "true", "yes")
)
TELEMETRY_MIRROR_MAX_AGE_SEC = int(
    os.getenv("TELEMETRY_MIRROR_MAX_AGE_SEC", "900")
)  # 15 minutes
TELEMETRY_MIRROR_MIN_BALANCE = float(
    os.getenv("TELEMETRY_MIRROR_MIN_BALANCE", "0.0")
)

STABLES = {"USDC", "USDT", "USD", "USDP", "DAI"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _get_telemetry() -> Dict[str, Any]:
    """
    Fetch the latest telemetry snapshot from the running Bus via HTTP.

    Expects the /api/telemetry/last endpoint added in wsgi.py to return:
        {"ok": true, "data": {"agent_id": ..., "by_venue": {...}, "flat": {...}, "ts": ...}}
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

def mirror_telemetry_once() -> None:
    if not TELEMETRY_MIRROR_ENABLED:
        info("telemetry_mirror: disabled via TELEMETRY_MIRROR_ENABLED=0.")
        return

    if not SHEET_URL:
        warn("telemetry_mirror: SHEET_URL not set; cannot write Wallet_Monitor.")
        return

    tel = _get_telemetry()
    if not tel:
        info("telemetry_mirror: no telemetry snapshot; skipping.")
        return

    ts = tel.get("ts")
    by_venue = tel.get("by_venue") or {}

    age_sec = None
    if ts is not None:
        try:
            age_sec = max(0.0, time.time() - float(ts))
        except Exception:
            age_sec = None

    if age_sec is None:
        info("telemetry_mirror: snapshot has no valid ts; skipping.")
        return

    if age_sec > TELEMETRY_MIRROR_MAX_AGE_SEC:
        info(
            f"telemetry_mirror: snapshot age {int(age_sec)}s "
            f"> TELEMETRY_MIRROR_MAX_AGE_SEC={TELEMETRY_MIRROR_MAX_AGE_SEC}; skipping."
        )
        return

    now_str = _utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
            # Wallet_Monitor columns: Timestamp, Venue, Asset, Free, Locked, Quote
            rows.append([now_str, venue_u, asset_u, qf, 0.0, quote])

    if not rows:
        info("telemetry_mirror: no non-zero balances to mirror; nothing to do.")
        return

    # Use the shared sheets_append_rows helper (handles auth + retries).
    sheets_append_rows(SHEET_URL, WALLET_MONITOR_WS, rows)
    info(
        f"telemetry_mirror: mirrored {len(rows)} balances into {WALLET_MONITOR_WS}."
    )


def main() -> None:
    mirror_telemetry_once()


if __name__ == "__main__":
    main()
