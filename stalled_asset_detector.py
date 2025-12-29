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
import logging
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


# Module logger
logger = logging.getLogger(__name__)

# Sheet tab names
WALLET_MONITOR_TAB = os.getenv("WALLET_MONITOR_TAB", "Wallet_Monitor")

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
STALL_IGNORE_ASSETS    = {x.strip().upper() for x in os.getenv('STALL_DETECTOR_IGNORE_ASSETS', 'USD,USDC,USDT').split(',') if x and x.strip()}
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

def _normalize_record(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Wallet_Monitor rows into a predictable schema so downstream logic
    (Venue/Asset/Timestamp/Free/Locked/etc.) never breaks.

    Wallet_Monitor source rows may have:
      - different key casing
      - leading/trailing spaces in headers
      - missing keys
    This function never raises.
    """
    if not isinstance(r, dict):
        return {}

    # Normalize keys (strip whitespace, keep original values)
    rr: Dict[str, Any] = {}
    for k, v in r.items():
        if k is None:
            continue
        kk = str(k).strip()
        if not kk:
            continue
        rr[kk] = v

    # Provide canonical keys expected by the detector
    # Wallet_Monitor usually uses these exact headers:
    # Timestamp | Agent | Venue | Asset | Free | Locked | Class | Snapshot
    out: Dict[str, Any] = {}

    # Timestamp (allow a few variants)
    out["Timestamp"] = rr.get("Timestamp") or rr.get("timestamp") or rr.get("TS") or rr.get("ts") or ""

    # Venue / Asset
    out["Venue"] = rr.get("Venue") or rr.get("venue") or ""
    out["Asset"] = rr.get("Asset") or rr.get("asset") or ""

    # Balances
    out["Free"] = rr.get("Free") if "Free" in rr else rr.get("free", 0)
    out["Locked"] = rr.get("Locked") if "Locked" in rr else rr.get("locked", 0)

    # Optional context columns
    out["Class"] = rr.get("Class") or rr.get("class") or ""
    out["Agent"] = rr.get("Agent") or rr.get("agent") or ""

    return out

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
    """Load balances from Wallet_Monitor, returning *latest* row per (Venue, Asset).

    Wallet_Monitor is append-only. As it grows, iterating every historical row causes the
    stalled detector to emit duplicate anomalies. We collapse to the newest record per
    (venue, asset) key before classification.
    """
    try:
        ws = _get_ws(sh, WALLET_MONITOR_TAB)
        rows = ws.get_all_records()

        balances: List[Dict[str, Any]] = []
        for r in rows:
            r = _normalize_record(r)
            venue = (r.get("Venue") or "").strip().upper()
            asset = (r.get("Asset") or "").strip().upper()
            if not venue or not asset:
                continue

            ts_raw = r.get("Timestamp") or r.get("timestamp") or r.get("TS") or r.get("ts")
            ts = _parse_ts(ts_raw) or 0

            free = _safe_float(r.get("Free"))
            locked = _safe_float(r.get("Locked"))
            total = free + locked
            if total <= 0:
                continue

            balances.append({
                "venue": venue,
                "asset": asset,
                "free": free,
                "locked": locked,
                "total": total,
                "timestamp": ts,
                "class": (r.get("Class") or r.get("class") or "").strip().upper(),
                "agent": (r.get("Agent") or r.get("agent") or "").strip(),
            })

        latest: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for b in balances:
            k = (b["venue"], b["asset"])
            prev = latest.get(k)
            if (prev is None) or (b.get("timestamp", 0) >= prev.get("timestamp", 0)):
                latest[k] = b

        out = list(latest.values())
        out.sort(key=lambda x: (x.get("venue", ""), x.get("asset", "")))
        return out
    except Exception as e:
        logger.exception("[stalled_asset_detector] failed to load wallet balances: %s", e)
        return []


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

def append_policy_rows(sh, new_records: List[Dict[str, Any]]) -> int:
    """
    Write stalled-asset rows into Policy_Log, but avoid unbounded growth.

    Strategy:
      • Keep all existing Policy_Log rows where Action != "STALL_DETECTOR".
      • Drop all old STALL_DETECTOR rows.
      • Append this run's stalled-asset rows.
      • Rewrite the sheet (header + compacted rows).

    That way:
      • Other policy events remain a true append-only audit log.
      • Stalled-asset detector behaves like a "latest snapshot" view.
    """
    if not new_records:
        return 0

    ws = _get_ws(sh, POLICY_LOG_WS)
    header = _ensure_policy_header(ws)
    col_index = {name: i for i, name in enumerate(header)}

    def dicts_to_rows(records: List[Dict[str, Any]]) -> List[List[Any]]:
        out: List[List[Any]] = []
        for rec in records:
            row = [""] * len(header)
            for k, v in rec.items():
                if k not in col_index:
                    continue
                row[col_index[k]] = v
            out.append(row)
        return out

    # 1) Load existing rows and keep only NON-STALL_DETECTOR entries
    try:
        existing_dicts = ws.get_all_records()  # uses header row
    except Exception:
        existing_dicts = []

    base_dicts: List[Dict[str, Any]] = []
    for rec in existing_dicts:
        action = str(rec.get("Action", "")).strip().upper()
        if action == "STALL_DETECTOR":
            # Old stalled-asset rows are discarded; we will replace them.
            continue
        base_dicts.append(rec)

    # 2) Convert both existing (kept) and new stalled rows to raw row lists
    base_rows = dicts_to_rows(base_dicts)
    new_rows = dicts_to_rows(new_records)

    # 3) Rewrite the sheet: header + compacted rows
    ws.clear()
    ws.append_row(header, value_input_option="RAW")
    out_rows = base_rows + new_rows
    if out_rows:
        ws.append_rows(out_rows, value_input_option="RAW")

    return len(new_rows)

# ==== Core detection ====

def classify_balances(
    balances: List[Dict[str, Any]],
    last_trades: Dict[Tuple[str, str], datetime],
) -> List[Dict[str, Any]]:
    now = _utcnow()
    anomalies: List[Dict[str, Any]] = []

    for b in balances:
        asset = b["asset"]
        # Common quote currencies are expected to have no trade history; skip by default.
        if asset in STALL_IGNORE_ASSETS:
            continue

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
    """
    Turn classified anomalies into Policy_Log rows.

    We emit TWO kinds of rows:

      1) Anomaly row:
         - Action = "STALL_DETECTOR"
         - OK = FALSE
         - Reason / Notes describe the classification.

      2) (Optional) BUY suggestion row:
         - Action = "BUY"
         - OK = TRUE
         - Notes contains "auto_resized"
         - Decision.patched_intent contains a tiny suggested BUY
           that downstream guard + autotrader can inspect.

    BUY suggestions are only emitted for:
      - venue in {"COINBASE", "BINANCEUS"}
      - non-stable assets (asset not in STABLE_SYMBOLS)
    """
    now_iso = _utcnow().isoformat(timespec="seconds")

    # Default tiny order size (in USD) for suggestions; can be overridden by env.
    try:
        default_buy_usd = float(os.getenv("STALL_AUTOBUY_DEFAULT_USD", "11"))
    except Exception:
        default_buy_usd = 11.0

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
            "total": total,
            "age_days": age_days,
            "last_ts": last_ts,
        }

        # 1) Always emit the anomaly row (what you already have today)
        anomaly_row = {
            "Timestamp": now_iso,
            "Token": asset,
            "Action": "STALL_DETECTOR",
            "Amount_USD": "",
            "OK": False,
            "Reason": reason,
            "Patched": "{}",  # no sizing here
            "Venue": venue,
            "Quote": "",
            "Liquidity": "",
            "Cooldown_Min": "",
            "Notes": json.dumps(notes, sort_keys=True),
            "Intent_ID": "",
            "Symbol": "",
            "Decision": "{}",  # purely informational
            "Source": "bus/stalled_asset_detector",
        }
        rows.append(anomaly_row)

        # 2) Optional BUY suggestion row (for the autotrader, still shadow)
        v_up = (venue or "").upper()
        asset_up = (asset or "").upper()

        if v_up not in {"COINBASE", "BINANCEUS"}:
            continue
        if asset_up in STABLE_SYMBOLS:
            # We only suggest BUYs for non-stable assets.
            continue

        # Very small USD size; guard + budgets will clamp further if needed.
        amount_usd = default_buy_usd

        # For now we don't try to be clever about pairs — we just mark quote "USDT".
        # trade_guard only needs a non-zero price_usd for sizing; notional is in amount_usd.
        suggested_intent = {
            "token": asset_up,
            "venue": v_up,
            "quote": "USDT",
            "amount_usd": amount_usd,
            "price_usd": 1.0,
            "action": "BUY",
        }

        decision = {
            "ok": True,
            "reason": "ok",
            "patched_intent": suggested_intent,
            "flags": ["auto_resized", "stalled_suggestion"],
        }

        buy_row = {
            "Timestamp": now_iso,
            "Token": asset_up,
            "Action": "BUY",
            "Amount_USD": amount_usd,
            "OK": True,
            "Reason": "stalled_auto_resized",
            "Patched": json.dumps({"amount_usd": amount_usd}),
            "Venue": v_up,
            "Quote": "USDT",
            "Liquidity": "",
            "Cooldown_Min": "",
            # <<< IMPORTANT: this is what our autotrader looks for
            "Notes": "auto_resized",
            "Intent_ID": "",
            "Symbol": "",
            "Decision": json.dumps(decision, sort_keys=True),
            "Source": "bus/stalled_asset_detector",
        }
        rows.append(buy_row)

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

def run_stalled_asset_detector() -> None:
    """
    Adapter used by the boot scheduler (_safe_call).
    Simply delegates to main(), which runs a full scan.
    """
    main()

if __name__ == "__main__":
    main()
