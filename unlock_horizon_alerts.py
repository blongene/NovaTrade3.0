# unlock_horizon_alerts.py
import os
from datetime import datetime, timedelta

from utils import (
    with_sheet_backoff,
    get_ws,
    get_records_cached,
    ws_batch_update,
    send_telegram_message_dedup,
    ping_webhook_debug,
    safe_float,
)

SHEET_CLAIMS = "Claim_Tracker"
ENV_COOLDOWN_MIN = int(os.getenv("UNLOCK_HORIZON_COOLDOWN_MIN", "30"))
_ENV_LAST_RUN_FILE = "/tmp/nt_unlock_horizon.last"

def _recently_ran() -> bool:
    try:
        ts = float(open(_ENV_LAST_RUN_FILE, "r").read().strip())
        return (datetime.utcnow() - datetime.utcfromtimestamp(ts)) < timedelta(minutes=ENV_COOLDOWN_MIN)
    except Exception:
        return False

def _mark_ran():
    try:
        open(_ENV_LAST_RUN_FILE, "w").write(str(datetime.utcnow().timestamp()))
    except Exception:
        pass

def _col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

@with_sheet_backoff
def run_unlock_horizon_alerts():
    print("🔔 Running Unlock Horizon Alerts...")

    # simple guard so redeploy storms don’t refire immediately
    if _recently_ran():
        print("⏳ Unlock Horizon: cooldown active; skipping.")
        return

    try:
        # 1) bulk read once, using cache to keep reads low
        ws = get_ws(SHEET_CLAIMS)
        rows = get_records_cached(SHEET_CLAIMS, ttl_s=120)  # one cached read

        if not rows:
            print("ℹ️ No rows in Claim_Tracker.")
            _mark_ran()
            return

        # map headers → column index (1-based)
        headers = ws.row_values(1)
        hidx = {h: i+1 for i, h in enumerate(headers)}

        # ensure we have the columns we’ll read/write
        need = ["Token", "Unlock Date", "Days Since Unlock", "Status", "Last Alerted"]
        missing = [c for c in need if c not in hidx]
        if missing:
            print(f"ℹ️ Claim Tracker missing columns: {', '.join(missing)}; skipping.")
            _mark_ran()
            return

        now = datetime.utcnow()
        updates = []
        alerts = []

        for i, rec in enumerate(rows, start=2):  # start at row 2
            token = (rec.get("Token") or "").strip().upper()
            if not token:
                continue

            unlock_raw = (rec.get("Unlock Date") or "").strip()
            status = (rec.get("Status") or "").strip().upper()
            last_alerted = (rec.get("Last Alerted") or "").strip()

            # skip if already resolved/claimed
            if status in {"RESOLVED", "CLAIMED", "ARCHIVED"}:
                continue

            # parse unlock date (expect ISO-ish)
            try:
                if not unlock_raw:
                    continue
                # be tolerant of 'YYYY-MM-DD' or ISO with time
                dt = datetime.fromisoformat(unlock_raw.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                # not a valid date; skip quietly
                continue

            days_since = (now - dt).days
            # write Days Since Unlock
            a1_days = f"{_col_letter(hidx['Days Since Unlock'])}{i}"
            updates.append({"range": a1_days, "values": [[str(days_since)]]})

            # alert once per day max
            should_alert = False
            if days_since >= 0:
                if not last_alerted:
                    should_alert = True
                else:
                    try:
                        la = datetime.fromisoformat(last_alerted.replace("Z", "+00:00")).replace(tzinfo=None)
                        should_alert = (now.date() > la.date())
                    except Exception:
                        should_alert = True

            if should_alert:
                alerts.append(f"• {token} unlocked {days_since}d ago (on {dt.date()})")
                a1_last = f"{_col_letter(hidx['Last Alerted'])}{i}"
                updates.append({"range": a1_last, "values": [[now.isoformat(timespec='seconds')]]})

        # 2) batch write once
        if updates:
            ws_batch_update(ws, updates)
            print(f"✅ Unlock horizon check complete. {len(updates)} cells updated, {len(alerts)} alert(s) prepared.")
        else:
            print("✅ Unlock horizon check complete. 0 rows updated, 0 alerts sent.")

        # 3) single de-duped Telegram summary (optional)
        if alerts:
            body = "🔔 <b>Unlock Horizon</b>\n" + "\n".join(alerts)
            send_telegram_message_dedup(body, key="unlock_horizon_daily", ttl_min=60)

        _mark_ran()

    except Exception as e:
        # soft-fail & set cooldown so we don’t thrash
        print(f"❌ Error in run_unlock_horizon_alerts: {e}")
        ping_webhook_debug(f"❌ Unlock Horizon error: {e}")
        _mark_ran()
