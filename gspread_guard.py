# gspread_guard.py — NT3 guard that patches Worksheet IO to use our backoff + buckets
# Safe to import multiple times; patches only once.

from utils import (
    _ws_get_all_records, _ws_get_all_values, _ws_get,
    ws_update, ws_batch_update, ws_append_row,
    sheets_gate, info, warn,
)

try:
    from gspread.models import Worksheet
except Exception as e:
    # If gspread isn't available yet, just no-op so boot doesn't die.
    def _noop(): ...
    warn(f"gspread_guard: cannot import gspread Worksheet ({e}); guard disabled")
else:
    if not getattr(Worksheet, "_nt3_guard_patched", False):
        # Keep originals for fallback
        _orig_get_all_records = Worksheet.get_all_records
        _orig_get_all_values  = Worksheet.get_all_values
        _orig_get             = Worksheet.get
        _orig_update          = Worksheet.update
        _orig_batch_update    = Worksheet.batch_update
        _orig_append_row      = Worksheet.append_row

        def _guard_get_all_records(self, *a, **k):
            try:
                # our utils version is budgeted + backoffed
                return _ws_get_all_records(self)
            except Exception as e:
                warn(f"gspread_guard.get_all_records: fallback ({e})")
                return _orig_get_all_records(self, *a, **k)

        def _guard_get_all_values(self, *a, **k):
            try:
                return _ws_get_all_values(self)
            except Exception as e:
                warn(f"gspread_guard.get_all_values: fallback ({e})")
                return _orig_get_all_values(self, *a, **k)

        def _guard_get(self, range_a1, *a, **k):
            try:
                return _ws_get(self, range_a1)
            except Exception as e:
                warn(f"gspread_guard.get('{range_a1}'): fallback ({e})")
                return _orig_get(self, range_a1, *a, **k)

        def _guard_update(self, range_a1, values=None, *a, **k):
            try:
                return ws_update(self, range_a1, values)
            except Exception as e:
                warn(f"gspread_guard.update('{range_a1}'): fallback ({e})")
                return _orig_update(self, range_a1, values, *a, **k)

        def _guard_batch_update(self, data, *a, **k):
            try:
                return ws_batch_update(self, data)
            except Exception as e:
                warn(f"gspread_guard.batch_update: fallback ({e})")
                return _orig_batch_update(self, data, *a, **k)

        def _guard_append_row(self, values, *a, **k):
            try:
                return ws_append_row(self, values)
            except Exception as e:
                warn(f"gspread_guard.append_row: fallback ({e})")
                return _orig_append_row(self, values, *a, **k)

        # Apply patches
        Worksheet.get_all_records = _guard_get_all_records
        Worksheet.get_all_values  = _guard_get_all_values
        Worksheet.get             = _guard_get
        Worksheet.update          = _guard_update
        Worksheet.batch_update    = _guard_batch_update
        Worksheet.append_row      = _guard_append_row

        # Mark once
        setattr(Worksheet, "_nt3_guard_patched", True)
        try:
            info("gspread_guard: Worksheet methods patched (budget + backoff + cache)")
        except Exception:
            pass
