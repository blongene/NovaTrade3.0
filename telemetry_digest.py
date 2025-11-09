
# telemetry_digest.py — Phase 9D
# Pulls /api/telemetry/last (local) and writes a heartbeat row to NovaHeartbeat.
# If telemetry age > threshold, posts a Telegram alert (deduped).
import os, time
from datetime import datetime, timezone

HEARTBEAT_WS = os.getenv("HEARTBEAT_WS", "NovaHeartbeat")
HEARTBEAT_ALERT_MIN = int(os.getenv("HEARTBEAT_ALERT_MIN", "90"))  # minutes
SHEET_URL = os.getenv("SHEET_URL", "")
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

try:
    from utils import get_gspread_client, send_telegram_message_dedup, warn, info
except Exception:
    get_gspread_client = None
    send_telegram_message_dedup = None
    def warn(x): print("[telemetry_digest] WARN:", x)
    def info(x): print("[telemetry_digest] INFO:", x)

def _http_get(url, timeout=6):
    try:
        import requests
        r = requests.get(url, timeout=timeout)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return {}

def _send_tg(text, key):
    if send_telegram_message_dedup:
        try:
            send_telegram_message_dedup(text, key=key, ttl_min=120)
            return
        except Exception:
            pass
    if BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
                timeout=8,
            )
        except Exception:
            pass

def _ensure_ws(sh, name, headers):
    try:
        ws = sh.worksheet(name)
        vals = ws.get_all_values()
        if not vals:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws
    except Exception:
        ws = sh.add_worksheet(title=name, rows=2000, cols=max(8, len(headers)+2))
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

def run_telemetry_digest():
    if not SHEET_URL:
        warn("SHEET_URL missing; abort.")
        return

    # 1) Pull telemetry from local Bus (best-effort)
    port = os.getenv("PORT", "10000")
    url = f"http://127.0.0.1:{port}/api/telemetry/last"
    j = _http_get(url) or {}
    age_sec = j.get("age_sec")
    agent = j.get("agent_id") or (j.get("agent") if isinstance(j.get("agent"), str) else None) or (j.get("agent") or "")
    flat = j.get("flat") or (j.get("telemetry", {}).get("flat") if isinstance(j.get("telemetry"), dict) else {}) or {}
    by_venue = j.get("by_venue") or (j.get("telemetry", {}).get("by_venue") if isinstance(j.get("telemetry"), dict) else {}) or {}

    # 2) Append to NovaHeartbeat
    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        ws = _ensure_ws(sh, HEARTBEAT_WS, ["Timestamp","Agent","Age_sec","Flat_Tokens","Venues","Note"])
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([ts, agent or "", "" if age_sec is None else int(age_sec), len(flat), len(by_venue), ""], value_input_option="USER_ENTERED")
        info("Heartbeat row appended.")
    except Exception as e:
        warn(f"heartbeat append failed: {e}")

    # 3) Alert if stale
    try:
        if age_sec is not None:
            age_min = age_sec / 60.0
            if age_min > HEARTBEAT_ALERT_MIN:
                _send_tg(f"⚠️ Edge heartbeat stale: {int(age_min)} min (>{HEARTBEAT_ALERT_MIN} min)", key=f"hb:{int(age_min)}")
    except Exception:
        pass

if __name__ == "__main__":
    run_telemetry_digest()
