# vault_review_alerts.py — NT3.0 (quota-safe, utils-first, legacy-friendly)
import os, time, random
from datetime import datetime, timezone

from utils import (
    with_sheet_backoff,
    with_sheets_gate,
    get_ws_cached,
    get_all_records_cached,
    ws_batch_update,
    send_telegram_message_dedup,
    str_or_empty,
)

# ---- Config -----------------------------------------------------------------
STATS_TAB          = os.getenv("VAULT_REVIEW_STATS_TAB", "Rotation_Stats")
TAG_COL_CANDIDATES = [  # we’ll accept either name
    os.getenv("VAULT_REVIEW_TAG_COL", "Memory Tag"),
    "Vault Tag",
]
LAST_REVIEWED_COL  = os.getenv("VAULT_REVIEW_LAST_COL", "Last Reviewed")
ALERT_DEDUP_TTL_MIN = int(os.getenv("VAULT_REVIEW_ALERT_TTL_MIN", "30"))  # 30 min
JITTER_MIN_S        = float(os.getenv("VAULT_REVIEW_JITTER_MIN_S", "0.10"))
JITTER_MAX_S        = float(os.getenv("VAULT_REVIEW_JITTER_MAX_S", "0.80"))

# ---- Small helpers ----------------------------------------------------------
def _a1_col(n: int) -> str:
    """1-based column index to letters"""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def _idx_or_none(header: list, name: str):
    try:
        return header.index(name) + 1  # 1-based
    except ValueError:
        return None

def _first_present(header: list, names: list[str]):
    for n in names:
        i = _idx_or_none(header, n)
        if i:
            return i, n
    return None, None

# ---- Main -------------------------------------------------------------------
@with_sheet_backoff
def run_vault_review_alerts():
    """
    Rule (default):
      Alert when a row’s tag is "⚠️ Never Vaulted" (or equivalent)
      AND "Last Reviewed" is blank.
    Then stamp "Last Reviewed" with current UTC time (batch write).
    """
    print("📬 Vault Review Alerts…")
    time.sleep(random.uniform(JITTER_MIN_S, JITTER_MAX_S))  # avoid boot pileup

    # Read once (cached, quota-safe)
    ws = get_ws_cached(STATS_TAB, ttl_s=30)
    rows = get_all_records_cached(STATS_TAB, ttl_s=180) or []
    values = ws.get_all_values()  # single call, wrapped by utils’ backoff
    if not values:
        print(f"ℹ️ {STATS_TAB} is empty; nothing to review.")
        return

    header = values[0]
    token_idx = _idx_or_none(header, "Token")
    if not token_idx:
        print(f"⚠️ {STATS_TAB} missing 'Token' header; skipping.")
        return

    tag_idx, tag_name = _first_present(header, TAG_COL_CANDIDATES)
    if not tag_idx:
        print(f"⚠️ {STATS_TAB} missing tag column ({' or '.join(TAG_COL_CANDIDATES)}); skipping.")
        return

    last_idx = _idx_or_none(header, LAST_REVIEWED_COL)
    # If "Last Reviewed" is missing, create the header now (batch once).
    header_updates = []
    if not last_idx:
        last_idx = len(header) + 1
        header_updates.append({"range": f"{_a1_col(last_idx)}1", "values": [[LAST_REVIEWED_COL]]})
        header.append(LAST_REVIEWED_COL)

    updates = []
    alerts  = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Use the record dicts for logic, but row numbers from values for writes.
    # values[1:] are data rows; enumerate for 1-based index (headers = row 1)
    for i, rec in enumerate(rows, start=2):
        # Defensive: ensure we can address same row in values
        if i - 1 >= len(values):
            break  # sheet shrank between calls; safe exit

        token = str_or_empty(rec.get("Token")).upper()
        if not token:
            continue

        # Accept either Memory Tag or Vault Tag, whichever exists in the sheet
        vault_tag = str_or_empty(rec.get(tag_name))
        last_rev  = str_or_empty(rec.get(LAST_REVIEWED_COL))

        # Default rule: never vaulted & never reviewed -> alert and stamp time
        should_alert = (vault_tag == "⚠️ Never Vaulted") and (last_rev == "")
        if not should_alert:
            continue

        a1 = f"{_a1_col(last_idx)}{i}"
        updates.append({"range": a1, "values": [[now_iso]]})
        alerts.append(f"• {token}: needs first vault review")

    if not header_updates and not updates and not alerts:
        print("✅ Vault Review: no changes needed.")
        return

    # Batch writes (consume write tokens explicitly to play nice with other jobs)
    with with_sheets_gate("write", tokens=max(1, len(header_updates) + len(updates))):
        payload = []
        if header_updates: payload.extend(header_updates)
        if updates:        payload.extend(updates)
        if payload:
            ws_batch_update(ws, payload)
            print(f"✅ Vault Review updated {len(updates)} cell(s){' + header' if header_updates else ''}.")

    # Optional Telegram summary (de-duped)
    if alerts:
        body = "🧰 <b>Vault Review Alerts</b>\n" + "\n".join(alerts)
        send_telegram_message_dedup(body, key="vault_review_alerts", ttl_min=ALERT_DEDUP_TTL_MIN)

    print("✅ Vault review alerts pass complete.")
