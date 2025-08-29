# rebuy_weight_calculator.py
import os
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utils import with_sheet_backoff, str_or_empty, safe_float, ping_webhook_debug

SHEET_URL = os.getenv("SHEET_URL")
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
STATS_SHEET = "Rotation_Stats"       # input/output
WEIGHT_COL_NAME = "Rebuy Weight"     # output column we maintain


# ---------- Helpers ----------

def _col_letter(n: int) -> str:
    """1-based column index -> letters, e.g. 1 -> A, 27 -> AA."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


@with_sheet_backoff
def _open_ws(title: str):
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", SCOPE)
    client = gspread.authorize(creds)
    sh = client.open_by_url(SHEET_URL)
    return sh.worksheet(title)


def _compute_weight(memory_tag: str, perf_val, max_rebuy_roi, avg_rebuy_roi):
    """
    Keep it simple and stable:
      - Drive weight primarily from Memory Tag.
      - Nudge by performance and historical rebuy ROI if present.
    """
    tag = str_or_empty(memory_tag)

    # Base by tag (lower = less urgency to rebuy, higher = more urgency)
    if "Big Win" in tag:
        base = 0.3
    elif "Small Win" in tag:
        base = 0.7
    elif "Break-Even" in tag:
        base = 1.0
    elif "Big Loss" in tag:
        base = 1.7
    elif "Loss" in tag:
        base = 1.3
    else:
        base = 1.0  # Unknown/empty

    # Small nudges (bounded) if numeric values are present
    # Performance > 1.0 suggests strong follow-through → nudge down slightly
    if isinstance(perf_val, (int, float)):
        if perf_val > 1.0:
            base -= min(0.2, (perf_val - 1.0) * 0.05)
        elif perf_val < 1.0 and perf_val >= 0:
            base += min(0.2, (1.0 - perf_val) * 0.05)

    # If historical rebuy ROI shows promise, add a gentle positive nudge
    if isinstance(max_rebuy_roi, (int, float)) and max_rebuy_roi > 50:
        base += 0.1
    if isinstance(avg_rebuy_roi, (int, float)) and avg_rebuy_roi > 15:
        base += 0.1

    # Clamp for sanity
    if base < 0.1:
        base = 0.1
    if base > 2.0:
        base = 2.0

    # Nice 2-decimal value
    return round(base, 2)


# ---------- Main ----------

def run_rebuy_weight_calculator():
    print("🧠 Rebuy Weights…")
    print("▶️ Rebuy weight calc …")

    try:
        ws = _open_ws(STATS_SHEET)

        # One read of all values (fast & quota-friendly)
        rows = ws.get_all_values()
        if not rows:
            print("⚠️ Empty Rotation_Stats sheet.")
            return

        header = rows[0]
        hidx = {name: i + 1 for i, name in enumerate(header)}  # 1-based for A1 helpers

        # Ensure our output column exists
        weight_col = hidx.get(WEIGHT_COL_NAME)
        if weight_col is None:
            header.append(WEIGHT_COL_NAME)
            # Write full header once; this is a single update
            ws.update("A1", [header])
            weight_col = len(header)
            print(f"ℹ️ Created '{WEIGHT_COL_NAME}' column at index {weight_col}")

        # Optional/used inputs (safe if missing)
        token_ix        = hidx.get("Token")
        memtag_ix       = hidx.get("Memory Tag")
        perf_ix         = hidx.get("Performance")             # follow-up / initial
        max_rebuy_ix    = hidx.get("Max Rebuy ROI")
        avg_rebuy_ix    = hidx.get("Avg Rebuy ROI")

        updates = []
        updated_count = 0

        for r, row in enumerate(rows[1:], start=2):  # start=2 because header is row 1
            token = str_or_empty(row[token_ix - 1]).upper() if token_ix else ""
            if not token:
                continue

            memtag = row[memtag_ix - 1] if memtag_ix else ""
            perf_v = safe_float(row[perf_ix - 1], None) if perf_ix else None
            max_r  = safe_float(row[max_rebuy_ix - 1], None) if max_rebuy_ix else None
            avg_r  = safe_float(row[avg_rebuy_ix - 1], None) if avg_rebuy_ix else None

            weight = _compute_weight(memtag, perf_v, max_r, avg_r)

            a1 = f"{_col_letter(weight_col)}{r}"  # DO NOT duplicate sheet name here
            updates.append({"range": a1, "values": [[weight]]})
            updated_count += 1

        # Batch-write in chunks to be gentle on quotas
        if updates:
            CHUNK = 200
            for i in range(0, len(updates), CHUNK):
                ws.batch_update(updates[i:i + CHUNK], value_input_option="USER_ENTERED")

        print(f"✅ Rebuy Weights updated for {updated_count} tokens.")

    except gspread.exceptions.APIError as e:
        # Surface quota hits clearly in logs, but don’t crash the loop
        print(f"❌ Error in run_rebuy_weight_calculator: {e}")
        try:
            ping_webhook_debug(f"⚠️ Rebuy Weight Calculator: Sheets API error → {e}")
        except Exception:
            pass
    except Exception as e:
        print(f"❌ Error in run_rebuy_weight_calculator: {e}")
