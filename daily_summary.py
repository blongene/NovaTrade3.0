# daily_summary.py — Phase-5 Telegram digest (runs ~09:00 ET)
# Improvements:
# • Env-driven service account path (SVC_JSON)
# • Robust Google Sheets retries/backoff for 429/5xx
# • Safer parsing of booleans/timestamps
# • One-per-day de-dupe (per ET day)
# • Optional Bus outbox snapshot if BASE_URL is provided
# • Clean HTML escaping + consistent timeouts

import os, time, json, math, hashlib, pathlib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ---- Config (env) -----------------------------------------------------------

BOT_TOKEN         = os.getenv("BOT_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
SHEET_URL         = os.getenv("SHEET_URL", "")
SVC_JSON          = os.getenv("SVC_JSON", "sentiment-log-service.json")

VAULT_WS_NAME     = os.getenv("VAULT_INTELLIGENCE_WS", "Vault Intelligence")
POLICY_LOG_WS     = os.getenv("POLICY_LOG_WS", "Policy_Log")

# Optional: include a tiny Bus health line if this is set
BASE_URL          = os.getenv("BASE_URL", "").rstrip("/")

# Change if you want a different send window / label
DAILY_HOUR_ET     = int(os.getenv("DAILY_SUMMARY_HOUR_ET", "9"))

HTTP_TIMEOUT      = 15
MAX_RETRIES       = 5
RETRY_BASE_SEC    = 1.5

# De-dupe marker lives on ephemeral disk (fine for Render)
DEDUP_DIR         = pathlib.Path("/tmp/daily-summary")
DEDUP_DIR.mkdir(parents=True, exist_ok=True)


# ---- Utilities --------------------------------------------------------------

def _to_bool(v) -> bool:
    s = str(v).strip().lower()
    return s in ("true", "yes", "y", "1")

def _safe_iso(ts: str):
    """Parse ISO timestamp into a UTC-aware datetime, or None."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        # If the sheet gives us a naive timestamp, assume it is UTC.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SVC_JSON, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

def _retry(op, *args, **kwargs):
    # Simple exponential backoff retry for Sheets/API
    for i in range(1, MAX_RETRIES + 1):
        try:
            return op(*args, **kwargs)
        except Exception as e:
            # Common Sheets “429: Rate Limit Exceeded”/5xx -> backoff
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
        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg_html, "parse_mode": "HTML", "disable_web_page_preview": True},
        timeout=HTTP_TIMEOUT,
    )
    try:
        ok = r.json().get("ok", False)
    except Exception:
        ok = r.ok
    return ok

def _dedup_key(et_date: datetime, payload: str) -> pathlib.Path:
    # one-per-day key for the ET date + payload hash (makes it resilient to code changes)
    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return DEDUP_DIR / f"phase5_{et_date:%Y-%m-%d}_{h}.sent"

def _send_once_per_day(msg_html: str):
    et_now = datetime.now(ZoneInfo("America/New_York"))
    key = _dedup_key(et_now.date() if isinstance(et_now, datetime) else et_now, msg_html)
    if key.exists():
        print("Daily summary already sent today. (dedup)")
        return
    if _tg_send(msg_html):
        key.write_text(str(int(datetime.now(tz=timezone.utc).timestamp())))
        print("Daily summary sent.")
    else:
        print("Telegram send failed (no dedup file written).")

def _bus_outbox_snapshot():
    if not BASE_URL:
        return None
    try:
        r = requests.get(f"{BASE_URL}/api/debug/outbox", timeout=HTTP_TIMEOUT)
        j = r.json()
        # Expect shape: {"done":N,"leased":N,"queued":N}
        d = int(j.get("done", 0))
        l = int(j.get("leased", 0))
        q = int(j.get("queued", 0))
        return (d, l, q)
    except Exception:
        return None

def _load_recent_policy_log(ws, hours: int = 24) -> list[dict]:
    """
    Load recent decisions from Policy_Log for the last `hours`.
    Column assumptions:
      A: timestamp (ISO)
      B: asset
      C: source (e.g. STALL_DETECTOR, MANUAL_REBUY, etc.)
      D: ?
      E: approved? (YES/NO)
      F: reason/notes
      G: venue (optional)
    """
    since_utc = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = ws.get_all_values()
    header, data = rows[0], rows[1:]
    out: list[dict] = []

    for row in data:
        if not row or not row[0]:
            continue
        t = _safe_iso(row[0])
        # If we couldn't parse the timestamp, or it's too old, skip
        if not t or t < since_utc:
            continue

        asset = row[1]
        source = row[2]
        approved_flag = (row[4] or "").strip().upper() == "YES"
        reason = (row[5] or "").strip()
        venue = (row[6] or "").strip()

        out.append({
            "ts": t,
            "asset": asset,
            "source": source,
            "approved": approved_flag,
            "reason": reason,
            "venue": venue,
        })

    return out

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
        # handle various header spellings just in case
        val = r.get("rebuy_ready")
        if val is None:
            val = r.get("Rebuy_Ready")
        if _to_bool(val):
            ready += 1
    total = len(vi)

    # Policy approvals/denials in last 24h
    since = datetime.utcnow() - timedelta(hours=24)
    appr = 0
    den = 0
    reasons = {}

    try:
        pl_ws = _retry(sh.worksheet, POLICY_LOG_WS)
        pl = _retry(pl_ws.get_all_records)
    except Exception:
        pl = []

    for r in pl:
        ts = r.get("Timestamp") or r.get("timestamp") or ""
        t = _safe_iso(ts)
        if not t or t < since:
            continue

        ok = r.get("OK")
        ok_b = _to_bool(ok)
        reason = (r.get("Reason") or r.get("reason") or "ok").strip() or "ok"

        if ok_b:
            appr += 1
        else:
            den += 1
            reasons[reason] = reasons.get(reason, 0) + 1

    # Top 3 denial reasons
    top_denials = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:3]
    reason_str = ", ".join([f"{k} ({v})" for k, v in top_denials]) if top_denials else "—"

    # Optional Bus outbox snapshot
    outbox_line = ""
    ob = _bus_outbox_snapshot()
    if ob:
        d, l, q = ob
        outbox_line = f"\nBus Outbox: done <code>{d}</code>, leased <code>{l}</code>, queued <code>{q}</code>"

    # Compose message
    et_now = datetime.now(ZoneInfo("America/New_York"))
    mode = os.getenv("REBUY_MODE", os.getenv("MODE", "dryrun"))
    msg = (
        "🧠 <b>Phase-5 Daily</b>\n"
        f"Date (ET): <code>{et_now:%Y-%m-%d}</code> around {DAILY_HOUR_ET:02d}:00\n"
        f"Vault Intelligence: <b>{ready}</b>/<b>{total}</b> rebuy-ready\n"
        f"Policy (24h): <b>{appr}</b> approved / <b>{den}</b> denied\n"
        f"Top denials: {reason_str}\n"
        f"Mode: <code>{mode}</code>{outbox_line}"
    )

    _send_once_per_day(msg)


# ---- CLI entry --------------------------------------------------------------

if __name__ == "__main__":
    # Run it immediately. You’ll typically wire this to a daily 09:00 ET job.
    daily_phase5_summary()
