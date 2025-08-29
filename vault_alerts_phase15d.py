# vault_alerts.py
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import with_sheet_backoff, send_telegram_message_dedup

SHEET_URL   = os.getenv("SHEET_URL")
SCOPE       = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
STATS_SHEET = "Rotation_Stats"

# --- helpers ---------------------------------------------------------------

def _to_float(val):
    """
    Safely parse ROI-like strings:
      '12', '12.5', '12%', ' -70 % ', 'N/A', ''  -> float or None
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # common non-numeric sentinels
    if s.upper() in {"N/A", "NA", "NONE", "-"}:
        return None
    # strip % and spaces
    s = s.replace("%", "").strip()
    try:
        return float(s)
    except Exception:
        return None

def _pick_roi(row):
    """
    Pick the best available ROI field from a Rotation_Stats row.
    Order of preference: 'Follow-up ROI' > 'ROI' > 'ROI (%)'
    """
    for k in ("Follow-up ROI", "ROI", "ROI (%)"):
        if k in row:
            v = _to_float(row.get(k))
            if v is not None:
                return v, k
    return None, None

def _bucket_from_roi(roi):
    """
    Map ROI to alert bucket (same semantics you’ve been using elsewhere).
    Returns (bucket_name, emoji) or (None, None) if no alert needed.
    """
    if roi is None:
        return None, None
    if roi >= 200:
        return "big_win", "🟢 Big Win"
    if 25 <= roi < 200:
        # Info-level; usually not an alert. Skip to keep noise (and 429s) down.
        return None, None
    if -24 <= roi <= 24:
        return None, None
    if -70 <= roi < -25:
        return "loss", "🔻 Loss"
    if roi <= -71:
        return "big_loss", "🔴 Big Loss"
    return None, None

@with_sheet_backoff
def _open_stats_ws():
    creds  = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    sh     = client.open_by_url(SHEET_URL)
    return sh.worksheet(STATS_SHEET)

# --- main ------------------------------------------------------------------

def run_vault_alerts():
    """
    Scan Rotation_Stats once with caching and send de-duped Telegram alerts
    when ROI crosses important buckets. No writes → minimal risk of 429.
    """
    try:
        print("🔔 Running Vault Intelligence Alerts...")
        ws = _open_stats_ws()

        # Prefer the method injected by your utils installer, else fall back.
        get_rows = getattr(ws, "get_records_cached", None)
        if callable(get_rows):
            rows = get_rows(ttl_s=180)  # 3-minute cache window
        else:
            # one-shot read; still wrapped by with_sheet_backoff on _open_stats_ws
            rows = ws.get_all_records()

        alerts_sent = 0

        for row in rows:
            token = str(row.get("Token", "")).strip().upper()
            if not token:
                continue

            roi, src = _pick_roi(row)
            if roi is None:
                # Be quiet unless you want debug:
                # print(f"⚠️ ROI parse skip for {token}: {row.get('Follow-up ROI') or row.get('ROI') or row.get('ROI (%)')}")
                continue

            bucket, label = _bucket_from_roi(roi)
            if not bucket:
                continue

            # De-dupe per token+bucket so we don't spam during frequent loops
            dedupe_key = f"vault_alert:{token}:{bucket}"
            msg = (
                f"{label}\n"
                f"<b>{token}</b> ROI now <b>{roi:.2f}%</b> (source: {src}).\n"
                f"— Vault Intelligence"
            )
            # conservative TTL to avoid repeats; adjust as you like
            try:
                send_telegram_message_dedup(msg, key=dedupe_key, ttl_min=60)
                alerts_sent += 1
            except Exception as te:
                # Never fail the run on Telegram hiccups
                print(f"⚠️ Telegram send skipped for {token}: {te}")

        print(f"✅ Vault alert check complete. {alerts_sent} Telegram(s) sent.")

    except Exception as e:
        # Keep this quiet & resilient; the decorator already retries reads.
        print(f"❌ Error in run_vault_alerts: {e}")
