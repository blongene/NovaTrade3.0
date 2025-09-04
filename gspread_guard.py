# gspread_guard.py — patch gspread.Worksheet with budget + backoff (version-safe)

import random, time
from typing import Any

try:
    # gspread <=5.x
    from gspread.models import Worksheet
except Exception:
    try:
        # gspread >=6 may re-export from top-level
        from gspread import Worksheet  # type: ignore
    except Exception as e:
        Worksheet = None  # type: ignore
        WorksheetImportError = e

from utils import sheets_gate, warn, info, sanitize_range, BACKOFF_BASE_S, BACKOFF_MAX_S, BACKOFF_JIT_S

_PATCHED = False

def _with_backoff(op_name: str, fn, *args, **kwargs):
    """Lightweight retry loop mirroring utils.with_sheet_backoff semantics."""
    import gspread
    delay = float(BACKOFF_BASE_S) + random.random() * float(BACKOFF_JIT_S)
    while True:
        try:
            # choose budget bucket based on operation name
            mode = "write" if op_name in {"update","batch_update","append_row"} else "read"
            with sheets_gate(mode=mode, tokens=1):
                return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            msg = str(e).lower()
            if any(s in msg for s in ("rate limit", "quota", "429", "500", "503", "user rate limit")):
                warn(f"Sheets backoff ({op_name}): {e}")
                time.sleep(delay)
                delay = min(float(BACKOFF_MAX_S), delay * 1.8)
                continue
            raise
        except Exception as e:
            if any(x in str(e).lower() for x in ("timed out", "connection reset", "temporarily", "unavailable")):
                warn(f"Transient error ({op_name}): {e}; retrying…")
                time.sleep(delay)
                delay = min(float(BACKOFF_MAX_S), delay * 1.8)
                continue
            raise

def _patch() -> None:
    global _PATCHED
    if _PATCHED:
        return
    if Worksheet is None:
        warn(f"gspread_guard: cannot import gspread Worksheet ({WorksheetImportError}); guard disabled")
        return

    # keep originals
    _orig_get             = Worksheet.get
    _orig_get_all_values  = Worksheet.get_all_values
    _orig_get_all_records = Worksheet.get_all_records
    _orig_update          = Worksheet.update
    _orig_batch_update    = Worksheet.batch_update
    _orig_append_row      = Worksheet.append_row

    # ---- READS ----
    def _guard_get(self: Worksheet, range_a1: str | None = None, *args: Any, **kwargs: Any):
        # Accept optional range; gspread may pass none
        return _with_backoff("get", _orig_get, self, range_a1, *args, **kwargs)

    def _guard_get_all_values(self: Worksheet, *args: Any, **kwargs: Any):
        return _with_backoff("get_all_values", _orig_get_all_values, self, *args, **kwargs)

    def _guard_get_all_records(self: Worksheet, *args: Any, **kwargs: Any):
        # signature varies by gspread version; forward verbatim
        return _with_backoff("get_all_records", _orig_get_all_records, self, *args, **kwargs)

    # ---- WRITES ----
    def _guard_update(self: Worksheet, range_a1: str, values, *args: Any, **kwargs: Any):
        return _with_backoff("update", _orig_update, self, sanitize_range(range_a1), values, *args, **kwargs)

    def _guard_batch_update(self: Worksheet, body, *args: Any, **kwargs: Any):
        return _with_backoff("batch_update", _orig_batch_update, self, body, *args, **kwargs)

    def _guard_append_row(self: Worksheet, values, *args: Any, **kwargs: Any):
        return _with_backoff("append_row", _orig_append_row, self, values, *args, **kwargs)

    # apply patches
    Worksheet.get             = _guard_get            # type: ignore[assignment]
    Worksheet.get_all_values  = _guard_get_all_values # type: ignore[assignment]
    Worksheet.get_all_records = _guard_get_all_records# type: ignore[assignment]
    Worksheet.update          = _guard_update         # type: ignore[assignment]
    Worksheet.batch_update    = _guard_batch_update   # type: ignore[assignment]
    Worksheet.append_row      = _guard_append_row     # type: ignore[assignment]

    _PATCHED = True
    info("gspread_guard: Worksheet methods patched (budget + backoff)")

# patch on import
_patch()
