# vault_alerts_phase15d.py — bullet-proof len() guard + type-safe alerts
import os, traceback, builtins as _builtins
from datetime import datetime, timedelta

from utils import (
    get_ws_cached, send_telegram_message_dedup,
    str_or_empty, to_float, warn
)

# ---- len() guard for this module only ---------------------------------------
import builtins as _bltn
__REAL_LEN = _bltn.len
def len(x):  # noqa: A001 (intentional local override)
    try:
        return __REAL_LEN(x)
    except TypeError:
        return __REAL_LEN(str(x))
# -----------------------------------------------------------------------------

# ---- Module-local safe len() wrapper (affects only this file) ---------------
_raw_len = _builtins.len
def len(x):  # noqa: A001  (intentional shadow for this module only)
    try:
        return _raw_len(x)
    except TypeError:
        return _raw_len(str(x))

# --- Config via env ----------------------------------------------------------
VAULT_TAB            = os.getenv("VAULT_SHEET_TAB", "Presale_Stream")
CLAIM_FLAG_COL_NAME  = os.getenv("CLAIM_FLAG_COLUMN", "Claim_Flag")  # header name
ALERT_MODE           = (os.getenv("ALERT_MODE", "telegram") or "telegram").lower()  # telegram|sheet|both
DEBUG_VAULT_ALERTS   = os.getenv("DEBUG_VAULT_ALERTS", "0") == "1"

# --- Helpers -----------------------------------------------------------------
def _s(row, key) -> str:
    return str_or_empty(row.get(key))

def _n(row, key):
    return to_float(row.get(key))

def _parse_date(s: str):
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

def _resolve_claim_flag_col_idx(ws):
    try:
        hdr = ws.row_values(1)
        for i, h in enumerate(hdr, start=1):
            if str_or_empty(h).lower() == CLAIM_FLAG_COL_NAME.lower():
                return i
    except Exception:
        pass
    return None

def _normalize_row(row: dict) -> dict:
    """
    Make every value safely stringifiable; numeric reads still available via _n().
    Prevents any stray len(value) from blowing up.
    """
    norm = {}
    for k, v in row.items():
        if v is None:
            norm[k] = ""
        else:
            # keep as-is for numeric use via _n(); but string ops will be safe due to our len wrapper
            norm[k] = v
    return norm

def _should_alert(row) -> bool:
    """
    Rules:
      - Token present
      - Status mentions 'vault'
      - Unlock date in [T-1d .. T+0]  OR claim flag already READY
    """
    token = _s(row, "Token")
    if len(token) == 0:
        return False

    claim_flag = _s(row, CLAIM_FLAG_COL_NAME).upper()
    if claim_flag == "READY":
        return True

    status = _s(row, "Status")
    vaulted = "vault" in status.lower() if status else False

    unlock_s = _s(row, "Unlock_Date") or _s(row, "Unlock Date")
    dt = _parse_date(unlock_s)
    if not dt:
        return False

    today = datetime.utcnow().date()
    d = dt.date()
    in_window = (d <= today) and ((today - d) <= timedelta(days=1))
    return bool(vaulted and in_window)

def _format_alert(row) -> str:
    token  = _s(row, "Token")
    roi    = _n(row, "ROI") or _n(row, "ROI %") or 0.0
    days   = _n(row, "Days Held") or 0
    status = _s(row, "Status") or "—"
    return (
        f"🔔 *Vault Unlock*: {token}\n"
        f"• ROI: {roi:.2f}%\n"
        f"• Days held: {int(days)}\n"
        f"• Status: {status}\n"
        f"• Action: Claim & restake? (or rotate)"
    )

def _write_ready(ws, row_idx_1based):
    if ALERT_MODE not in ("sheet", "both"):
        return
    try:
        col_idx = _resolve_claim_flag_col_idx(ws)
        if col_idx is None:
            return
        ws.update_cell(row_idx_1based, col_idx, "READY")  # backoff-wrapped via utils
    except Exception as e:
        warn(f"READY write failed (row {row_idx_1based}): {e}")

# --- Entry -------------------------------------------------------------------
def run_vault_alerts():
    """
    Type-safe, len-proof vault alert scan.
    One bad row never aborts the job.
    """
    try:
        ws = get_ws_cached(VAULT_TAB, ttl_s=30)
        rows = ws.get_all_records()
    except Exception as e:
        warn(f"Vault alerts: sheet load failed: {e}")
        return

    alerts = []
    for i, raw in enumerate(rows, start=2):  # row 1 = header
        try:
            row = _normalize_row(raw)
            if _should_alert(row):
                alerts.append((i, _format_alert(row)))
        except Exception as e:
            if DEBUG_VAULT_ALERTS:
                tb = traceback.format_exc(limit=1)
                warn(f"Vault alerts row {i} error: {e} | row={raw} | tb={tb}")
            else:
                warn(f"Vault alerts row {i} error: {e}")

    for row_idx, msg in alerts:
        if ALERT_MODE in ("telegram", "both"):
            try:
                send_telegram_message_dedup(msg, key=f"vault_alert_row{row_idx}")
            except Exception as e:
                warn(f"Telegram send failed for row {row_idx}: {e}")
        _write_ready(ws, row_idx)

    if alerts:
        send_telegram_message_dedup(f"✅ Vault alerts sent: {len(alerts)}", key="vault_alerts_summary")
    else:
        send_telegram_message_dedup("ℹ️ No vault alerts today.", key="vault_alerts_none")
