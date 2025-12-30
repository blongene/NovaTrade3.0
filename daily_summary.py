#!/usr/bin/env python3
# daily_summary.py — Phase-5 Telegram digest (runs ~09:00 ET)
#
# Key features:
# • Env-driven service account path (SVC_JSON)
# • Robust Google Sheets retries/backoff for 429/5xx
# • Safer parsing of booleans/timestamps/strings (no .strip() on ints)
# • One-per-day de-dupe (per ET day)
# • Optional Bus outbox snapshot if BASE_URL is provided
# • Clean HTML escaping + consistent timeouts
#
# Phase 22B note:
# - Fixes a prior syntax/escape issue ("unexpected character after line continuation character")
# - Keeps behavior identical otherwise

from __future__ import annotations

import os
import time
import hashlib
import pathlib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, Tuple, Optional

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ---- Config (env) -----------------------------------------------------------

TTL_READ_S = int(os.getenv("TTL_READ_S", "120") or "120")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SHEET_URL = os.getenv("SHEET_URL", "")
SVC_JSON = os.getenv("SVC_JSON", "sentiment-log-service.json")

VAULT_WS_NAME = os.getenv("VAULT_INTELLIGENCE_WS", "Vault Intelligence")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")
WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")

STABLE_TOKENS = {"USD", "USDT", "USDC", "ZUSD"}

# Optional: include a tiny Bus health line if this is set
BASE_URL = (os.getenv("BASE_URL", "") or "").rstrip("/")

# Change if you want a different send window / label
DAILY_HOUR_ET = int(os.getenv("DAILY_SUMMARY_HOUR_ET", "9"))

HTTP_TIMEOUT = 15
MAX_RETRIES = 5
RETRY_BASE_SEC = 1.5

# De-dupe marker lives on ephemeral disk (fine for Render)
DEDUP_DIR = pathlib.Path("/tmp/daily-summary")
DEDUP_DIR.mkdir(parents=True, exist_ok=True)


# ---- Helpers / utilities ----------------------------------------------------


def _to_bool(v) -> bool:
    """Best-effort boolean parser."""
    s = str(v).strip().lower()
    return s in ("true", "yes", "y", "1")


def _safe_iso(ts: str):
    """Parse ISO timestamp into a UTC-aware datetime object; None on failure."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _safe_stripped(v, default: str = "") -> str:
    """
    Safely call .strip() on arbitrary values.

    Handles ints, floats, None, etc. Always returns a string.
    """
    if v is None:
        return default
    try:
        return str(v).strip()
    except Exception:
        return default


def _open_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SVC_JSON, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)


def _retry(op, *args, **kwargs):
    """Simple exponential backoff retry for Sheets/API calls."""
    for i in range(1, MAX_RETRIES + 1):
        try:
            return op(*args, **kwargs)
        except Exception:
            if i == MAX_RETRIES:
                raise
            sleep = RETRY_BASE_SEC * (2 ** (i - 1)) + (0.1 * i)
            time.sleep(sleep)


def _tg_send(msg_html: str):
    if not (BOT_TOKEN and TELEGRAM_CHAT_ID):
        print("Telegram not configured.")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=HTTP_TIMEOUT,
    )
    try:
        ok = r.json().get("ok", False)
    except Exception:
        ok = r.ok
    return ok


def _dedup_key(et_date, payload: str) -> pathlib.Path:
    """One-per-day key for the ET date + payload hash."""
    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return DEDUP_DIR / f"phase5_{et_date:%Y-%m-%d}_{h}.sent"


def _send_once_per_day(msg_html: str):
    et_now = datetime.now(ZoneInfo("America/New_York"))
    key = _dedup_key(et_now.date(), msg_html)
    if key.exists():
        print("Daily summary already sent today. (dedup)")
        return
    if _tg_send(msg_html):
        key.write_text(str(int(datetime.now(tz=timezone.utc).timestamp())))
        print("Daily summary sent.")
    else:
        print("Telegram send failed (no dedup file written).")


def _bus_outbox_snapshot():
    """Optional Bus outbox health line."""
    if not BASE_URL:
        return None
    try:
        r = requests.get(f"{BASE_URL}/api/debug/outbox", timeout=HTTP_TIMEOUT)
        j = r.json()
        d = int(j.get("done", 0))
        l = int(j.get("leased", 0))
        q = int(j.get("queued", 0))
        return (d, l, q)
    except Exception:
        return None


def _safe_float_or_zero(val):
    """Best-effort float conversion; returns 0.0 on any failure."""
    try:
        if val is None or val == "":
            return 0.0
        return float(val)
    except Exception:
        return 0.0


def _latest_wallet_snapshot(sheet):
    """
    Read Wallet_Monitor and return (snapshot_ts_utc_str, summary_line) or None.
    """
    try:
        ws = _retry(sheet.worksheet, WALLET_MONITOR_WS)
        rows = _retry(ws.get_all_records)
    except Exception:
        return None

    if not rows:
        return None

    best_ts = None
    best_rows = []

    for r in rows:
        ts_raw = r.get("Timestamp") or r.get("timestamp") or ""
        t = _safe_iso(ts_raw)
        if not t:
            continue
        t = t.astimezone(timezone.utc)
        if best_ts is None or t > best_ts:
            best_ts = t
            best_rows = [r]
        elif t == best_ts:
            best_rows.append(r)

    if not best_ts or not best_rows:
        return None

    by_venue: Dict[str, Dict[str, float]] = {}
    for r in best_rows:
        venue = _safe_stripped(r.get("Venue") or r.get("venue") or "?") or "?"
        asset = _safe_stripped(r.get("Asset") or r.get("asset") or "").upper()
        free = _safe_float_or_zero(r.get("Free") or r.get("free"))
        info = by_venue.setdefault(venue, {"tokens": 0, "stable": 0.0})
        info["tokens"] += 1
        if asset in STABLE_TOKENS:
            info["stable"] += free

    parts = []
    for venue in sorted(by_venue.keys()):
        info = by_venue[venue]
        tokens = info["tokens"]
        stable = info["stable"]
        if stable > 0:
            parts.append(f"{venue}: {tokens} tokens, ~{stable:.2f} stable")
        else:
            parts.append(f"{venue}: {tokens} tokens")

    if not parts:
        return None

    ts_str = best_ts.strftime("%Y-%m-%d %H:%M")
    summary_line = "; ".join(parts)
    return ts_str, summary_line


# ---- Core logic -------------------------------------------------------------


def daily_phase5_summary():
    if not SHEET_URL:
        print("SHEET_URL missing; abort.")
        return

    sh = _retry(_open_sheet)

    # Vault Intelligence (ready / total)
    try:
        vi_ws = _retry(sh.worksheet, VAULT_WS_NAME)
        vi = _retry(vi_ws.get_all_records)
    except Exception:
        vi = []

    ready = 0
    for r in vi:
        val = r.get("rebuy_ready")
        if val is None:
            val = r.get("Rebuy_Ready")
        if _to_bool(val):
            ready += 1
    total = len(vi)

    # Policy approvals/denials in last 24h (UTC, offset-aware)
    appr = 0
    den = 0
    reasons: Dict[str, int] = {}

    try:
        pl_ws = _retry(sh.worksheet, POLICY_LOG_WS)
        pl = _retry(pl_ws.get_all_records)
    except Exception:
        pl = []

    now_utc = datetime.now(timezone.utc)
    since = now_utc - timedelta(hours=24)

    for r in pl:
        ts = r.get("Timestamp") or r.get("timestamp") or ""
        t = _safe_iso(ts)
        if not t:
            continue
        t = t.astimezone(timezone.utc)
        if t < since:
            continue

        ok_b = _to_bool(r.get("OK"))
        raw_reason = r.get("Reason")
        if raw_reason in (None, ""):
            raw_reason = r.get("reason")
        reason = _safe_stripped(raw_reason, default="ok") or "ok"

        if ok_b:
            appr += 1
        else:
            den += 1
            reasons[reason] = reasons.get(reason, 0) + 1

    top_denials = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:3]
    reason_str = ", ".join([f"{k} ({v})" for k, v in top_denials]) if top_denials else "—"

    # Wallet snapshot line
    wallet_line = ""
    snapshot = None
    try:
        snapshot = _latest_wallet_snapshot(sh)
    except Exception:
        snapshot = None
    if snapshot:
        ts_str, desc = snapshot
        wallet_line = f"Wallets (snapshot {ts_str} UTC): {desc}\n"

    # Optional Bus outbox snapshot
    outbox_line = ""
    ob = _bus_outbox_snapshot()
    if ob:
        d, l, q = ob
        outbox_line = (
            f"\nBus Outbox: done <code>{d}</code>, "
            f"leased <code>{l}</code>, queued <code>{q}</code>"
        )

    et_now = datetime.now(ZoneInfo("America/New_York"))
    mode = os.getenv("REBUY_MODE", os.getenv("MODE", "dryrun"))

    msg = (
        "🧠 <b>NovaTrade Daily Summary</b>\n"
        f"Date (ET): <code>{et_now:%Y-%m-%d}</code> around {DAILY_HOUR_ET:02d}:00\n"
        f"Vault Intelligence: <b>{ready}</b>/<b>{total}</b> rebuy-ready\n"
        f"Policy (24h): <b>{appr}</b> approved / <b>{den}</b> denied\n"
        f"{wallet_line}"
        f"Top denials: {reason_str}\n"
        f"Mode: <code>{mode}</code>{outbox_line}"
    )

    _send_once_per_day(msg)


# ---- Scheduler compatibility ------------------------------------------------

def run_daily_summary():
    """Compatibility wrapper for scheduler/main imports."""
    return daily_phase5_summary()


if __name__ == "__main__":
    daily_phase5_summary()
