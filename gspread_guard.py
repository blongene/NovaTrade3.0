# gspread_guard.py — patch gspread.Worksheet with budget + backoff (version-safe)

import random, time, functools
from typing import Any, Tuple

# --- Import Worksheet across gspread versions --------------------------------
WorksheetImportError = None
try:
    # gspread <= 5.x
    from gspread.models import Worksheet  # type: ignore
except Exception:
    try:
        # Some builds re-export at top level (defensive)
        from gspread import Worksheet  # type: ignore
    except Exception as e:  # pragma: no cover
        Worksheet = None  # type: ignore
        WorksheetImportError = e

from utils import (
    sheets_gate, warn, info, sanitize_range,
    BACKOFF_BASE_S, BACKOFF_MAX_S, BACKOFF_JIT_S
)

# Phase 22A: optional Postgres shadow-write mirror for append-only tabs.
# Best-effort only: it must never break Sheets writes.
try:
    from db_mirror import mirror_append
except Exception:
    mirror_append = None  # type: ignore

# -----------------------------------------------------------------------------
# Backoff helper mirrors utils semantics but stays local to wrappers.
# -----------------------------------------------------------------------------
def _with_backoff(op_name: str, fn, *args, **kwargs):
    import gspread
    delay = float(BACKOFF_BASE_S) + random.random() * float(BACKOFF_JIT_S)
    while True:
        try:
            mode = "write" if op_name in {"update", "batch_update", "append_row"} else "read"
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

# -----------------------------------------------------------------------------
# Argument normalization for Worksheet.get
# Prevents "multiple values for argument 'range_name'" across call styles.
# -----------------------------------------------------------------------------
def _normalize_get_args(args: Tuple[Any, ...], kwargs: dict) -> Tuple[Any, Tuple[Any, ...], dict]:
    """
    Returns (range_name, rest_args, kwargs_without_dup)
    Ensures we never pass both positional and keyword 'range_name'.
    """
    kw = dict(kwargs or {})
    if args and "range_name" in kw:
        kw.pop("range_name", None)

    if args:
        range_name = args[0]
        rest = args[1:]
    else:
        range_name = kw.pop("range_name", None)
        rest = ()

    # Sanitize if present
    if range_name is not None:
        try:
            range_name = sanitize_range(range_name)
        except Exception:
            # Be permissive; if sanitize rejects, keep original
            pass

    return range_name, rest, kw

# -----------------------------------------------------------------------------
# Patch once (idempotent) and wrap key Worksheet methods
# -----------------------------------------------------------------------------
def _patch() -> None:
    if Worksheet is None:
        warn(f"gspread_guard: cannot import gspread Worksheet ({WorksheetImportError}); guard disabled")
        return

    # Idempotent guard across imports/workers
    if getattr(Worksheet, "_nova_guard_patched", False):
        return

    # Keep originals
    _orig_get             = Worksheet.get
    _orig_get_all_values  = Worksheet.get_all_values
    _orig_get_all_records = Worksheet.get_all_records
    _orig_update          = Worksheet.update
    _orig_batch_update    = Worksheet.batch_update
    _orig_append_row      = Worksheet.append_row
    _orig_append_rows     = getattr(Worksheet, "append_rows", None)

    # ---- READS ----
    @functools.wraps(_orig_get)
    def _guard_get(self: Worksheet, *args: Any, **kwargs: Any):
        range_name, rest, kw = _normalize_get_args(args, kwargs)

        def _call():
            try:
                # Prefer positional call (covers most gspread versions)
                if range_name is None:
                    return _orig_get(self, *rest, **kw)
                return _orig_get(self, range_name, *rest, **kw)
            except TypeError:
                # Fallback: pass as keyword if this build prefers it
                if range_name is None:
                    return _orig_get(self, **kw)
                return _orig_get(self, range_name=range_name, **kw)

        return _with_backoff("get", _call)

    @functools.wraps(_orig_get_all_values)
    def _guard_get_all_values(self: Worksheet, *args: Any, **kwargs: Any):
        return _with_backoff("get_all_values", _orig_get_all_values, self, *args, **kwargs)

    @functools.wraps(_orig_get_all_records)
    def _guard_get_all_records(self: Worksheet, *args: Any, **kwargs: Any):
        return _with_backoff("get_all_records", _orig_get_all_records, self, *args, **kwargs)

    # ---- WRITES ----
    @functools.wraps(_orig_update)
    def _guard_update(self: Worksheet, range_a1: Any, values: Any, *args: Any, **kwargs: Any):
        try:
            range_a1 = sanitize_range(range_a1) if range_a1 is not None else range_a1
        except Exception:
            pass
        return _with_backoff("update", _orig_update, self, range_a1, values, *args, **kwargs)

    @functools.wraps(_orig_batch_update)
    def _guard_batch_update(self: Worksheet, body: Any, *args: Any, **kwargs: Any):
        return _with_backoff("batch_update", _orig_batch_update, self, body, *args, **kwargs)

    @functools.wraps(_orig_append_row)
    def _guard_append_row(self: Worksheet, values: Any, *args: Any, **kwargs: Any):
        res = _with_backoff("append_row", _orig_append_row, self, values, *args, **kwargs)
        # Phase 22A: best-effort DB shadow-write.
        try:
            if mirror_append is not None:
                title = getattr(self, "title", None) or "?"
                mirror_append(title, [values])
        except Exception:
            # Absolutely never break Sheets writes.
            pass
        return res

    if _orig_append_rows is not None:
        @functools.wraps(_orig_append_rows)
        def _guard_append_rows(self: Worksheet, values: Any, *args: Any, **kwargs: Any):
            """Guard + backoff for Worksheet.append_rows, plus optional DB mirror.

            values is typically a list of rows (list[list[Any]]).
            """
            res = _with_backoff("append_rows", _orig_append_rows, self, values, *args, **kwargs)
            if mirror_append is not None:
                try:
                    tab = getattr(self, "title", "")
                    mirror_append(tab, values)
                except Exception:
                    pass
            return res

    # Apply patches
    Worksheet.get             = _guard_get             # type: ignore[assignment]
    Worksheet.get_all_values  = _guard_get_all_values  # type: ignore[assignment]
    Worksheet.get_all_records = _guard_get_all_records # type: ignore[assignment]
    Worksheet.update          = _guard_update          # type: ignore[assignment]
    Worksheet.batch_update    = _guard_batch_update    # type: ignore[assignment]
    Worksheet.append_row      = _guard_append_row      # type: ignore[assignment]
    if _orig_append_rows is not None:
        Worksheet.append_rows = _guard_append_rows     # type: ignore[assignment]
    Worksheet._nova_guard_patched = True

    info("gspread_guard: Worksheet methods patched (budget + backoff)")

# Patch on import
_patch()
