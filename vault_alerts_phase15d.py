# vault_alerts_phase15d.py — hardened + len-safe
import os, traceback
from datetime import datetime, timedelta
from utils import (
    get_ws_cached, send_telegram_message_dedup,
    str_or_empty, to_float, safe_len, warn
)

VAULT_TAB           = os.getenv("VAULT_SHEET_TAB", "Presale_Stream")
CLAIM_FLAG_COL_NAME = os.getenv("CLAIM_FLAG_COLUMN", "Claim_Flag")
ALERT_MODE          = (os.getenv("ALERT_MODE", "telegram") or "telegram").lower()  # telegram|sheet|both
DEBUG_VAULT_ALERTS  = os.getenv("DEBUG_VAULT_ALERTS", "0") == "1"

def _n(row, key):
    return to_float(row.get(key), default=None)

def _parse_date(s):
    s = str_or_empty(s)
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None

def _resolve_col_idx(ws, header_name):
    try:
        hdr = ws.row_values(1)
        for i, h in enumerate(hdr, start=1):
            if str_or_empty(h).lower() == str_or_empty(header_name).lower():
                return i
    except Exception:
        pass
    return None

def _should_alert(row) -> bool:
    token = str_or_empty(row.get("Token"))
    if safe_len(token) == 0:
        return False

    # quick exits
    claim_flag = str_or_empty(row.get(CLAIM_FLAG_COL_NAME)).upper()
    if claim_flag == "READY":
        return True

    status = str_or_empty(row.get("Status"))
    vaulted = ("vault" in status.lower()) if status else False

    unlock_s = str_or_empty(row.get("Unlock_Date") or row.get("Unlock Date"))
    dt = _parse_date(unlock_s)
    if not dt:
        return False

    today = datetime.utcnow().date()
    in_window = (dt.date() <= today) and ((today - dt.date()) <= timedelta(days=1))
    return bool(vaulted and in_window)

def _format_alert(row) -> str:
    token  = str_or_empty(row.get("Token"))
    roi    = _n(row, "ROI") or _n(row, "ROI %") or 0.0
    days   = int(_n(row, "Days Held") or 0)
    status = str_or_empty(row.get("Status") or "—")
    return (
        f"🔔 *Vault Unlock*: {token}\n"
        f"• ROI: {roi:.2f}%\n"
        f"• Days held: {days}\n"
        f"• Status: {status}\n"
        f"• Action: Claim & restake? (or rotate)"
    )

def _write_ready(ws, row_idx_1based):
    if ALERT_MODE not in ("sheet", "both"):
        return
    try:
        col_idx = _resolve_col_idx(ws, CLAIM_FLAG_COL_NAME)
        if col_idx:
            ws.update_cell(row_idx_1based, col_idx, "READY")
    except Exception as e:
        warn(f"READY write failed (row {row_idx_1based}): {e}")

def run_vault_alerts():
    try:
        ws = get_ws_cached(VAULT_TAB, ttl_s=30)
        rows = ws.get_all_records()
    except Exception as e:
        warn(f"Vault alerts: sheet load failed: {e}")
        return

    alerts = []
    for i, row in enumerate(rows, start=2):
        try:
            if _should_alert(row):
                alerts.append((i, _format_alert(row)))
        except Exception as e:
            if DEBUG_VAULT_ALERTS:
                warn(f"Vault alerts row {i} error: {e} | row={row} | tb={traceback.format_exc(limit=1)}")
            else:
                warn(f"Vault alerts row {i} error: {e}")

    for row_idx, msg in alerts:
        if ALERT_MODE in ("telegram", "both"):
            try:
                send_telegram_message_dedup(msg, key=f"vault_alert_row{row_idx}")
            except Exception as e:
                warn(f"Telegram send failed for row {row_idx}: {e}")
        _write_ready(ws, row_idx)
