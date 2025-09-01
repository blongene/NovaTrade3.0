# rotation_log_header_guard.py
from utils import get_ws_cached, ws_update, warn

ROTATION_LOG = "Rotation_Log"
REQUIRED_HEADERS = ["Token", "Decision"]  # add more if you want to lock others in too

def _col_letter(n:int)->str:
    s=""
    while n:
        n,r=divmod(n-1,26)
        s=chr(65+r)+s
    return s

def ensure_rotation_log_headers():
    try:
        ws = get_ws_cached(ROTATION_LOG, ttl_s=10)
        header = ws.row_values(1) or []
        missing = [h for h in REQUIRED_HEADERS if h not in header]
        if not missing:
            return

        # Append missing headers at the end of row 1 in one batch write
        start_col = len(header) + 1
        writes = []
        for i, h in enumerate(missing, start=start_col):
            writes.append({"range": f"{_col_letter(i)}1", "values": [[h]]})
        ws.batch_update(writes, value_input_option="USER_ENTERED")
        print(f"✅ Rotation_Log header guard added: {', '.join(missing)}")
    except Exception as e:
        warn(f"Rotation_Log header guard failed: {e}")
