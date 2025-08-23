# memory_score_booster.py
import os
from utils import with_sheet_backoff, get_gspread_client

SHEET_URL = os.getenv("SHEET_URL")

def _open_sheet():
    return get_gspread_client().open_by_url(SHEET_URL)

@with_sheet_backoff
def _get_all(ws):
    return ws.get_all_values()

def _header_index_map(headers):
    return {h.strip(): i for i, h in enumerate(headers)}

def _normalize(s):
    return (s or "").strip().upper()

def get_memory_boost(token: str) -> int:
    """
    Looks up Rotation_Stats → Memory Weight (0..1) and converts to 0..+10 bonus.
    If not found, returns 0.
    """
    try:
        sh = _open_sheet()
        ws = sh.worksheet("Rotation_Stats")
        vals = _get_all(ws)
        if not vals:
            return 0
        h = _header_index_map(vals[0])
        t_col = h.get("Token")
        w_col = h.get("Memory Weight")
        if t_col is None or w_col is None:
            return 0

        want = _normalize(token)
        for r in vals[1:]:
            t = _normalize(r[t_col] if t_col < len(r) else "")
            if not t or t != want:
                continue
            try:
                w = float((r[w_col] if w_col < len(r) else "0").strip())
            except Exception:
                w = 0.0
            bonus = int(round(max(0.0, min(1.0, w)) * 10.0))
            return bonus
        return 0
    except Exception:
        return 0
