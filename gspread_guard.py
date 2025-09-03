# gspread_guard.py — NT3 guard (version-agnostic and lazy-patching)

import threading, time
from utils import (
    _ws_get_all_records, _ws_get_all_values, _ws_get,
    ws_update, ws_batch_update, ws_append_row,
    info, warn,
)

def _try_import_worksheet():
    try:
        from gspread.worksheet import Worksheet  # new path
        return Worksheet
    except Exception:
        try:
            from gspread.models import Worksheet  # old path
            return Worksheet
        except Exception as e:
            warn(f"gspread_guard: Worksheet class not available yet ({e}); will retry lazily")
            return None

def _patch(Worksheet):
    if not Worksheet or getattr(Worksheet, "_nt3_guard_patched", False):
        return

    _orig_get_all_records = Worksheet.get_all_records
    _orig_get_all_values  = Worksheet.get_all_values
    _orig_get             = Worksheet.get
    _orig_update          = Worksheet.update
    _orig_batch_update    = Worksheet.batch_update
    _orig_append_row      = Worksheet.append_row

    def _guard_get_all_records(self, *a, **k):
        try:
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

    Worksheet.get_all_records = _guard_get_all_records
    Worksheet.get_all_values  = _guard_get_all_values
    Worksheet.get             = _guard_get
    Worksheet.update          = _guard_update
    Worksheet.batch_update    = _guard_batch_update
    Worksheet.append_row      = _guard_append_row
    setattr(Worksheet, "_nt3_guard_patched", True)
    try: info("gspread_guard: Worksheet methods patched (budget + backoff)")
    except Exception: pass

# Try now; if not available, retry a few times in the background.
_W = _try_import_worksheet()
if _W:
    _patch(_W)
else:
    def _late():
        for _ in range(12):  # ~12s total
            time.sleep(1.0)
            W = _try_import_worksheet()
            if W:
                _patch(W); return
    threading.Thread(target=_late, daemon=True).start()
