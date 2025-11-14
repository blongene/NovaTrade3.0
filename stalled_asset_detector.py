#!/usr/bin/env python3
"""
stalled_asset_detector.py — NovaTrade 3.0

Purpose:
    Scan Wallet_Monitor + Trade_Log to find:
      - orphan assets (balance but no trade history),
      - stalled assets (no trade in N days),
      - stablecoins stuck in hubs.

    Results are appended to Policy_Log with a structured row per anomaly,
    and (optionally) summarized via Telegram using send_telegram_message_dedup.

Depends on:
    - Google Sheets (SHEET_URL, service account JSON via GOOGLE_APPLICATION_CREDENTIALS)
    - Existing tabs:
        * Wallet_Monitor
        * Trade_Log
        * Policy_Log

Safe to run ad-hoc or from a scheduler/cron. If nothing looks weird, it will
only print a short summary and exit.
"""

from __future__ import annotations
import os
import json
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Any

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Optional utilities (Telegram + logging). We degrade gracefully if missing.
try:
    from utils import send_telegram_message_dedup  # type: ignore
except Exception:  # pragma: no cover
    def send_telegram_message_dedup(*a, **k) -> bool:
        return False

try:
    from utils import warn  # type: ignore
except Exception:  # pragma: no cover
    def warn(msg: str) -> None:
        print(f"[stalled_asset_detector] WARN: {msg}")

# ==== Config ====

SHEET_URL = os.getenv("SHEET_URL")

WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")
TRADE_LOG_WS      = os.getenv("TRADE_LOG_WS", "Trade_Log")
POLICY_LOG_WS     = os.getenv("POLICY_LOG_WS", "Policy_Log")

STALL_DETECT_DAYS       = int(os.getenv("STALL_DETECT_DAYS", "7"))
STALL_ORPHAN_DAYS       = int(os.getenv("STALL_ORPHAN_DAYS", "30"))
STALL_MIN_BALANCE       = float(os.getenv("STALL_MIN_BALANCE", "0.000001"))
STALL_MIN_STABLE_BAL    = float(os.getenv("STALL_MIN_STABLE_BALANCE", "5.0"))
STALL_STABLE_DAYS       = int(os.getenv("STALL_STABLE_DAYS", "3"))

STABLE_SYMBOLS = {"USDC", "USDT", "USD", "USDP", "DAI"}


# ==== Helpers ====

def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _open_sheet():
    if not SHEET_URL:
        raise RuntimeError("SHEET_URL env var is not set")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    # Same default path pattern as other modules
    svc_path = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "/etc/secrets/sentiment-log-service.json",
    )

    creds = ServiceAccountCredentials.from_json_keyfile_name(svc_path, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)


def _get_ws(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        # For Policy_Log we want to auto-create, others we can treat as empty
        if title == POLICY_LOG_WS:
            return sh.add_worksheet(title=title, rows=2000, cols=20)
        raise


def _safe_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return 0.0
        return float(s.replace(",", ""))
    except Exception:
        return 0.0


def _parse_ts(s: str) -> datetime | None:
    """
    Accepts strings like:
        '2025-10-19 16:01:39'
        '2025-10-19T16:01:39Z'
    Returns timezone-aware UTC datetime where possible.
    """
    if not s:
        return None
    s = s.strip()
    # Replace space with 'T' and add Z if it looks ISO-like
    try_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]
    for fmt in try_formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _extract_base_from_symbol(symbol: str) -> str:
    """
    Turn 'BTCUSDT' or 'ETH/USDC' into 'BTC' / 'ETH'.
    Falls back to the raw symbol if we can't guess.
    """
    if not symbol:
        return ""
    s = symbol.strip().upper()
    if "/" in s:
        return s.split("/", 1)[0]

    for suffix in ("USDT", "USDC", "USD", "USDP", "DAI"):
        if s.endswith(suffix):
            return s[: -len(suffix)]
    return s


# ==== Data loaders ====

def load_wallet_balances(sh) -> List[Dict[str, Any]]:
    """
    From Wallet_Monitor:
        Timestamp | Venue | Asset | Free | Locked | Quote
    """
    try:
        ws = _get_ws(sh, WALLET_MONITOR_WS)
    except gspread.WorksheetNotFound:
        warn(f"Worksheet {WALLET_MONITOR_WS} not found; no balances to scan")
        return []

    rows = ws.get_all_values()
    if not rows:
        return []

    header = rows[0]
    hix = {h.strip(): i for i, h in enumerate(header) if h}

    needed = ["Timestamp", "Venue", "Asset", "Free", "Locked"]
    for col in needed:
        if col not in hix:
            warn(f"{WALLET_MONITOR_WS}: missing column '{col}', skipping")
            return []

    out = []
    for row in rows[1:]:
        def g(col: str) -> str:
            idx = hix.get(col)
            if idx is None or idx >= len(row):
                return ""
            return str(row[idx]).strip()

        ts = g("Timestamp")
        venue = g("Venue")
        asset = g("Asset")
        free = _safe_float(g("Free"))
        locked = _safe_float(g("Locked"))

        if not asset or not venue:
            continue

        total = free + locked
        if total <= 0:
            continue

        out.append(
            {
                "timestamp": ts,
                "venue": venue.upper(),
                "asset": asset.upper(),
                "free": free,
                "locked": locked,
                "total": total,
            }
        )
    return out


def load_last_trades(sh) -> Dict[Tuple[str, str], datetime]:
    """
    From Trade_Log:
        Timestamp | Venue | Symbol | Side | ...
    Returns:
        {(VENUE, BASE_ASSET): last_trade_ts}
    """
    try:
        ws = _get_ws(sh, TRADE_LOG_WS)
    except gspread.WorksheetNotFound:
        warn(f"Worksheet {TRADE_LOG_WS} not found; treating as no trade history")
        return {}

    rows = ws.get_all_values()
    if not rows:
        return {}

    header = rows[0]
    hix = {h.strip(): i for i, h in enumerate(header) if h}

    needed = ["Timestamp", "Venue", "Symbol"]
    for col in needed:
        if col not in hix:
            warn(f"{TRADE_LOG_WS}: missing column '{col}', skipping trade history")
            return {}

    last: Dict[Tuple[str, str], datetime] = {}
    for row in rows[1:]:
        def g(col: str) -> str:
            idx = hix.get(col)
            if idx is None or idx >= len(row):
                return ""
            return str(row[idx]).strip()

        ts_str = g("Timestamp")
        venue = g("Venue").upper()
        symbol = g("Symbol")

        if not venue or not symbol:
            continue

        ts = _parse_ts(ts_str)
        if ts is None:
            continue

        base = _extract_base_from_symbol(symbol)
        if not base:
            continue

        key = (venue, base)
        prev = last.get(key)
        if prev is None or ts > prev:
            last[key] = ts

    return last


# ==== Policy_Log integration ====

def _ensure_policy_header(ws) -> List[str]:
    rows = ws.get_all_values()
    if not rows:
        header = [
            "Timestamp",
            "Token",
            "Action",
            "Amount_USD",
            "OK",
            "Reason",
            "Patched",
            "Venue",
            "Quote",
            "Liquidity",
            "Cooldown_Min",
            "Notes",
            "Intent_ID",
            "Symbol",
            "Decision",
            "Source",
        ]
        ws.update("A1", [header])
        return header

    header = rows[0]
    # Normalize + ensure required columns exist
    existing = [h.strip() for h in header]
    wanted = [
        "Timestamp",
        "Token",
        "Action",
        "Amount_USD",
        "OK",
        "Reason",
        "Patched",
        "Venue",
        "Quote",
        "Liquidity",
        "Cooldown_Min",
        "Notes",
        "Intent_ID",
        "Symbol",
        "Decision",
        "Source",
    ]
    changed = False
    for w in wanted:
        if w not in existing:
            existing.append(w)
            changed = True

    if changed:
        ws.update("A1", [existing])
    return existing


def append_policy_rows(sh, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    ws = _get_ws(sh, POLICY_LOG_WS)
    header = _ensure_policy_header(ws)
    col_index = {name: i for i, name in enumerate(header)}

    out_rows: List[List[Any]] = []
    for r in rows:
        row = [""] * len(header)
        for k, v in r.items():
            if k not in col_index:
                continue
            idx = col_index[k]
            row[idx] = v
        out_rows.append(row)

    if out_rows:
        ws.append_rows(out_rows, value_input_option="RAW")
    return len(out_rows)


# ==== Core detection ====

def classify_balances(
    balances: List[Dict[str, Any]],
    last_trades: Dict[Tuple[str, str], datetime],
) -> List[Dict[str, Any]]:
    now = _utcnow()
    anomalies: List[Dict[str, Any]] = []

    for b in balances:
        asset = b["asset"]
        venue = b["venue"]
        total = float(b.get("total", 0.0))

        if asset in STABLE_SYMBOLS:
            min_bal = STALL_MIN_STABLE_BAL
        else:
            min_bal = STALL_MIN_BALANCE

        if total < min_bal:
            # Treat as dust, skip; we can add dust classification later if desired.
            continue

        key = (venue, asset)
        last_ts = last_trades.get(key)
        classification = None
        age_days = None

        if last_ts is None:
            classification = "orphan"
        else:
            delta = now - last_ts
            age_days = delta.total_seconds() / 86400.0

            if asset in STABLE_SYMBOLS:
                if age_days is not None and age_days >= STALL_STABLE_DAYS:
                    classification = "stable_stuck"
            else:
                if age_days is not None and age_days >= STALL_DETECT_DAYS:
                    classification = "stalled"

        if classification is None:
            continue

        anomalies.append(
            {
                "asset": asset,
                "venue": venue,
                "total": total,
                "last_ts": last_ts.isoformat() if last_ts else "",
                "age_days": age_days,
                "classification": classification,
            }
        )

    return anomalies


def build_policy_rows(anomalies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now_iso = _utcnow().isoformat(timespec="seconds")

    rows: List[Dict[str, Any]] = []
    for a in anomalies:
        asset = a["asset"]
        venue = a["venue"]
        total = a["total"]
        last_ts = a.get("last_ts") or ""
        age_days = a.get("age_days")
        classification = a["classification"]

        if age_days is None:
            age_str = "no trade history"
        else:
            age_str = f"{age_days:.1f}d since last trade"

        reason = (
            f"{classification} asset detected: {asset} on {venue}, "
            f"balance={total}, {age_str} (last_ts='{last_ts}')"
        )

        notes = {
            "classification": classification,
            "asset": asset,
            "venue": venue,
            "balance_total": total,
            "last_trade_ts": last_ts,
            "age_days": age_days,
            "thresholds": {
                "STALL_DETECT_DAYS": STALL_DETECT_DAYS,
                "STALL_ORPHAN_DAYS": STALL_ORPHAN_DAYS,
                "STALL_MIN_BALANCE": STALL_MIN_BALANCE,
                "STALL_MIN_STABLE_BALANCE": STALL_MIN_STABLE_BAL,
                "STALL_STABLE_DAYS": STALL_STABLE_DAYS,
            },
        }

        row = {
            "Timestamp": now_iso,
            "Token": asset,
            "Action": "STALL_DETECTOR",
            "Amount_USD": "",  # we don't compute USD here yet
            "OK": "NO",
            "Reason": reason,
            "Patched": "",
            "Venue": venue,
            "Quote": "",
            "Liquidity": "",
            "Cooldown_Min": "",
            "Notes": json.dumps(notes, separators=(",", ":")),
            "Intent_ID": "stalled_asset_detector",
            "Symbol": "",
            "Decision": classification,
            "Source": "bus/stalled_asset_detector",
        }
        rows.append(row)

    return rows


def send_telegram_summary(anomalies: List[Dict[str, Any]]) -> None:
    if not anomalies:
        return

    counts = {}
    for a in anomalies:
        c = a["classification"]
        counts[c] = counts.get(c, 0) + 1

    lines = ["[NovaTrade] Stalled Asset Detector"]
    for c, n in sorted(counts.items(), key=lambda kv: kv[0]):
        lines.append(f"- {c}: {n}")

    # Include up to 5 most severe examples
    top = anomalies[:5]
    lines.append("")
    lines.append("Top examples:")
    for a in top:
        asset = a["asset"]
        venue = a["venue"]
        total = a["total"]
        cls = a["classification"]
        age_days = a.get("age_days")
        if age_days is None:
            age_str = "no trade history"
        else:
            age_str = f"{age_days:.1f}d"
        lines.append(f"  • {cls}: {asset} on {venue}, bal={total}, age={age_str}")

    msg = "\n".join(lines)
    try:
        send_telegram_message_dedup(msg, key="stalled_asset_detector")
    except Exception as e:
        warn(f"Telegram summary failed: {e}")


def main() -> None:
    now = _utcnow().isoformat(timespec="seconds")
    print(f"[stalled_asset_detector] Starting scan at {now}")

    sh = _open_sheet()
    balances = load_wallet_balances(sh)
    print(f"[stalled_asset_detector] Loaded {len(balances)} wallet rows")

    last_trades = load_last_trades(sh)
    print(f"[stalled_asset_detector] Loaded {len(last_trades)} last-trade entries")

    anomalies = classify_balances(balances, last_trades)
    print(f"[stalled_asset_detector] Found {len(anomalies)} anomalies")

    if not anomalies:
        print("[stalled_asset_detector] No anomalies detected; exiting")
        return

    rows = build_policy_rows(anomalies)
    written = append_policy_rows(sh, rows)
    print(f"[stalled_asset_detector] Appended {written} rows to {POLICY_LOG_WS}")

    send_telegram_summary(anomalies)


if __name__ == "__main__":
    main()
