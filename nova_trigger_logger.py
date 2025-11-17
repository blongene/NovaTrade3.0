"""
nova_trigger_logger.py

Tiny helper to append a row to NovaTrigger_Log whenever NovaTrigger
processes an event (manual rebuy, SOS, etc.).

Sheet headers (expected):

    Timestamp | Trigger | Notes
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from utils import get_gspread_client, warn  # type: ignore

SHEET_URL = os.getenv("SHEET_URL", "").strip()
NOVATRIGGER_LOG_WS = os.getenv("NOVATRIGGER_LOG_WS", "NovaTrigger_Log").strip()


def _ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def log_nova_trigger(trigger: str, notes: Optional[str] = "") -> None:
    """
    Best-effort append into NovaTrigger_Log. Never raises.
    """
    if not SHEET_URL:
        return

    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        try:
            ws = sh.worksheet(NOVATRIGGER_LOG_WS)
        except Exception:
            ws = sh.add_worksheet(title=NOVATRIGGER_LOG_WS, rows=2000, cols=3)
            ws.append_row(
                ["Timestamp", "Trigger", "Notes"],
                value_input_option="USER_ENTERED",
            )
    except Exception as e:
        warn(f"nova_trigger_logger: failed to open {NOVATRIGGER_LOG_WS}: {e}")
        return

    try:
        ws.append_row(
            [_ts(), trigger, notes or ""],
            value_input_option="USER_ENTERED",
        )
    except Exception as e:
        warn(f"nova_trigger_logger: failed to append row: {e}")
