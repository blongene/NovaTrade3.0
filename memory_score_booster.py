# memory_score_booster.py — NT3.0 Phase-1 Polish (cache-first)
import os, time
from utils import get_values_cached, str_or_empty

# in-process soft cache to avoid re-parsing the sheet on every call
_CACHE = {"ts": 0.0, "map": {}}
_TTL_S = int(os.getenv("MEMORY_BOOST_TTL_SEC", "180"))  # 3 min default

def _build_map():
    vals = get_values_cached("Rotation_Stats", ttl_s=_TTL_S) or []
    if not vals:
        return {}
    h = {str_or_empty(c): i for i, c in enumerate(vals[0])}
    t_col = h.get("Token")
    w_col = h.get("Memory Weight")  # expected 0..1
    if t_col is None or w_col is None:
        return {}
    out = {}
    for row in vals[1:]:
        token = str_or_empty(row[t_col] if t_col < len(row) else "").upper()
        if not token:
            continue
        raw = str_or_empty(row[w_col] if w_col < len(row) else "")
        try:
            w = float(raw)
        except Exception:
            w = 0.0
        out[token] = max(0.0, min(1.0, w))
    return out

def get_memory_boost(token: str) -> int:
    """Returns an integer bonus 0..10 derived from Rotation_Stats.Memory Weight."""
    now = time.time()
    if now - _CACHE["ts"] > _TTL_S or not _CACHE["map"]:
        _CACHE["map"] = _build_map()
        _CACHE["ts"] = now
    weight = _CACHE["map"].get(str_or_empty(token).upper(), 0.0)
    return int(round(weight * 10.0))
