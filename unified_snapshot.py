"""
unified_snapshot.py — C-1 + B-2

Builds a normalized balance view in the Unified_Snapshot sheet:

  Timestamp | Venue | Asset | Free | Locked | Total | IsQuote | QuoteSymbol | Equity_USD

Source:
  • Wallet_Monitor (primary)
      columns: Timestamp, Venue, Asset, Free, Locked, Quote

Rules:
  • Total       = Free + Locked
  • IsQuote     = TRUE if Asset in {USDT, USDC, USD}, else FALSE
  • Equity_USD:
      - If IsQuote: Equity_USD = Total (1 quote ≈ 1 USD)
      - Else      : Equity_USD = Total * price_feed.get_price_usd(Asset, Quote or "USDT", Venue)
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import List, Tuple

import gspread  # type: ignore

from utils import get_gspread_client, warn  # type: ignore
from price_feed import get_price_usd  # B-2 oracle

SHEET_URL = os.getenv("SHEET_URL", "").strip()
SNAP_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")
WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")

QUOTE_ASSETS = {"USDT", "USDC", "USD"}


def _open_sheet() -> gspread.Spreadsheet:
    if not SHEET_URL:
        raise RuntimeError("unified_snapshot: SHEET_URL not set")
    gc = get_gspread_client()
    return gc.open_by_url(SHEET_URL)


def _safe_num(x) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def _load_wallet_rows(sh: gspread.Spreadsheet) -> List[Tuple[str, str, float, float, str]]:
    """
    Returns list of (venue, asset, free, locked, quote)
    """
    rows: List[Tuple[str, str, float, float, str]] = []
    try:
        ws = sh.worksheet(WALLET_MONITOR_WS)
        data = ws.get_all_records()
    except Exception as e:
        warn(f"unified_snapshot: unable to read Wallet_Monitor: {e}")
        return rows

    for r in data:
        venue = str(r.get("Venue", "")).strip().upper()
        asset = str(r.get("Asset", "")).strip().upper()
        if not venue or not asset:
            continue

        free = _safe_num(r.get("Free", 0))
        locked = _safe_num(r.get("Locked", 0))
        quote = str(r.get("Quote", "")).strip().upper()

        rows.append((venue, asset, free, locked, quote))

    return rows


def run_unified_snapshot() -> None:
    """
    Main entrypoint: build Unified_Snapshot from Wallet_Monitor.

    Safe to schedule periodically (e.g., every 10–15 minutes).
    """
    if not SHEET_URL:
        print("⚠️ unified_snapshot: SHEET_URL not set; aborting.")
        return

    print("📸 unified_snapshot: building Unified_Snapshot from Wallet_Monitor…")

    try:
        sh = _open_sheet()
    except Exception as e:
        print(f"❌ unified_snapshot: failed to open sheet: {e}")
        return

    wallet_rows = _load_wallet_rows(sh)
    if not wallet_rows:
        print("ℹ️ unified_snapshot: no Wallet_Monitor rows found; Unified_Snapshot will be empty (ok).")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    out_rows: List[list] = []

    for venue, asset, free, locked, quote in wallet_rows:
        total = free + locked
        is_quote = asset in QUOTE_ASSETS
        is_quote_str = "TRUE" if is_quote else "FALSE"

        equity_usd = ""

        if total > 0:
            if is_quote:
                # For quote assets, treat Total as USD value.
                equity_usd = total
            else:
                # B-2: lookup price via price_feed.
                # Prefer row's Quote if present; otherwise default to USDT.
                q = quote or "USDT"
                price = get_price_usd(asset, q, venue)
                if price is not None and price > 0:
                    equity_usd = total * price

        out_rows.append(
            [
                now,           # Timestamp
                venue,         # Venue
                asset,         # Asset
                free,          # Free
                locked,        # Locked
                total,         # Total
                is_quote_str,  # IsQuote
                quote or "",   # QuoteSymbol
                equity_usd,    # Equity_USD
            ]
        )

    # Write to Unified_Snapshot
    try:
        try:
            ws = sh.worksheet(SNAP_WS)
            ws.clear()
        except Exception:
            ws = sh.add_worksheet(title=SNAP_WS, rows=2000, cols=9)

        headers = [
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
        ws.append_row(headers, value_input_option="USER_ENTERED")

        if out_rows:
            ws.append_rows(out_rows, value_input_option="USER_ENTERED")
            print(f"✅ unified_snapshot: wrote {len(out_rows)} rows to {SNAP_WS}")
        else:
            print("ℹ️ unified_snapshot: nothing to write; snapshot contains headers only.")
    except Exception as e:
        print(f"⚠️ unified_snapshot: error writing {SNAP_WS}: {e}")
