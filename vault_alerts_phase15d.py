# vault_alerts_phase15d.py — hardened against type errors (len(int), None, etc.)
import os, time, traceback
from datetime import datetime, timedelta

from utils import (
    get_ws_cached, get_all_records_cached, ws_batch_update,
    send_telegram_message_dedup, str_or_empty, to_float, info, warn
)

VAULT_TAB = os.getenv("VAULT_SHEET_TAB", "Presale_Stream")
CLAIM_FLAG_COL = os.getenv("CLAIM_FLAG_COLUMN", "Claim_Flag")
ALERT_MODE = os.getenv("ALERT_MODE", "telegram").lower()   # telegram|sheet|both
DEBUG_VAULT_ALERTS = os.getenv("DEBUG_VAULT_ALERTS", "0") == "1"

def safe_len(x) -> int:
    """len() that never crashes on ints/floats/None."""
    try:
        return len(str_or_empty(x))
    except Exception:
        return 0

def get_str(row, key) -> str:
    return str_or_empty(row.get(key))

def get_num(row, key):
    return to_float(row.get(key))

def is_truthy(row, key):
    v = str_or_empty(row.get(key)).lower()
    return v in ("y", "yes", "true", "1", "ready", "ready_to_claim")

def _should_alert(row) -> bool:
    """
    Decide if this row should produce a vault alert.
    Examples of checks (adjust to your schema as needed):
      - Token is present
      - Status indicates vaulted or unlock imminent
      - Unlock window (T-1d .. T+0) or explicit READY flag
    """
    token = get_str(row, "Token")
    status = get_str(row, "Status")
    claim_flag = get_str(row, CLAIM_FLAG_COL)
    unlock_date_s = get_str(row, "Unlock_Date") or get_str(row, "Unlock Date")

    if safe_len(token) == 0:
        return False  # nothing to alert

    # If user already marked as ready to claim in sheet
    if claim_flag.upper() == "READY":
        return True

    # Basic status-based gate (adjust to your terms)
    vaulted = "vault" in status.lower() if status else False

    # Unlock window check
    should_by_date = False
    if safe_len(unlock_date_s) > 0:
        try:
            # Try ISO or common formats
            # If your sheet stores as 'YYYY-MM-DD' or 'MM/DD/YYYY', this should pass.
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(unlock_date_s, fmt)
                    break
                except ValueError:
                    dt = None
            if dt is None:
                # last resort: let datetime parse loosely
                dt = datetime.fromisoformat(unlock_date_s.replace("Z", ""))  # may still fail
            today = datetime.utcnow().date()
            d = dt.date()
            # Alert within T-1d to T+0
            should_by_date = (d <= today) and (today - d <= timedelta(days=1))
        except Exception:
            should_by_date = False

    return vaulted and should_by_date

def _format_alert(row) -> str:
    token = get_str(row, "Token")
    roi = get_num(row, "ROI") or get_num(row, "ROI %") or 0.0
    days = get_num(row, "Days Held") or 0
    status = get_str(row, "Status")
    return (
        f"🔔 *Vault Unlock*: {token}\n"
        f"• ROI: {roi:.2f}%\n"
        f"• Days held: {int(days)}\n"
        f"• Status: {status or '—'}\n"
        f"• Action: Claim & restake? (or rotate)"
    )

def _maybe_write_ready(ws, row_idx_1based):
    if ALERT_MODE in ("sheet", "both"):
        a1 = f"{CLAIM_FLAG_COL}{row_idx_1based}" if CLAIM_FLAG_COL.isalpha() else None
        # If CLAIM_FLAG_COL is a header name (typical), we need to resolve its column index
        # Resolve once using header row:
        hdr = ws.row_values(1)
        try:
            col_idx = next(i+1 for i,h in enumerate(hdr) if str_or_empty(h).lower() == CLAIM_FLAG_COL.lower())
            a1 = ws.title + "!" + ws._get_addr_int(row=row_idx_1based, col=col_idx)  # use internal to produce A1
        except Exception:
            # Fallback to plain header match via update_cell signature (one round trip)
            try:
                ws.update_cell(row_idx_1based, hdr.index(CLAIM_FLAG_COL)+1, "READY")
                return
            except Exception:
                a1 = None

        if a1:
            try:
                ws.update(a1, "READY")
            except Exception as e:
                warn(f"READY write failed for row {row_idx_1based}: {e}")

def run_vault_alerts():
    """
    Scans VAULT_TAB for imminent unlocks or READY flags and sends Telegram alerts,
    optionally writing Claim_Flag='READY'. Fully guarded against type errors.
    """
    try:
        ws = get_ws_cached(VAULT_TAB, ttl_s=30)
        rows = ws.get_all_records()  # wrapped with backoff in utils via decorator
    except Exception as e:
        warn(f"Vault alerts: sheet load failed: {e}")
        return

    alerts = []
    for i, row in enumerate(rows, start=2):  # 1 is header, so 2 = first data row
        try:
            if _should_alert(row):
                msg = _format_alert(row)
                alerts.append((i, msg))
        except Exception as e:
            # Harden per-row: never crash full job
            if DEBUG_VAULT_ALERTS:
                tb = traceback.format_exc(limit=1)
                warn(f"Vault alerts row {i} error: {e} | row={row} | tb={tb}")
            else:
                warn(f"Vault alerts row {i} error: {e}")

    # Deliver alerts
    for row_idx, msg in alerts:
        if ALERT_MODE in ("telegram", "both"):
            try:
                send_telegram_message_dedup(msg, key=f"vault_alert_row{row_idx}")
            except Exception as e:
                warn(f"Telegram send failed for row {row_idx}: {e}")
        try:
            _maybe_write_ready(ws, row_idx)
        except Exception as e:
            warn(f"READY write error row {row_idx}: {e}")

    # Optional summary ping (de-duped by key)
    if alerts:
        send_telegram_message_dedup(f"✅ Vault alerts sent: {len(alerts)}", key="vault_alerts_summary")
    else:
        # Quiet success message; change key TTL if you want daily only
        send_telegram_message_dedup("ℹ️ No vault alerts today.", key="vault_alerts_none")
