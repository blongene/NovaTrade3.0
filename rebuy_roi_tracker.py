# rebuy_roi_tracker.py — NT3.0 Render (429-safe, batch-only) — Phase 22B DB-aware
# Aggregates per-token "rebuy" performance from Rotation_Log and writes
# Rebuy Count / Win% / Avg ROI% into Rotation_Stats in a single batch.

import os, time, random
from typing import Any, Dict, List, Optional

from utils import (
    get_values_cached, get_ws, ws_batch_update, with_sheet_backoff,
    str_or_empty, to_float,
)

# Optional DB-aware reader (Sheets fallback is always available)
try:
    from utils import get_all_records_cached_dbaware  # type: ignore
except Exception:
    get_all_records_cached_dbaware = None  # type: ignore

# ---------------- Env config (override in Render) ----------------
LOG_TAB          = os.getenv("REBUY_LOG_TAB", "Rotation_Log")
STATS_TAB        = os.getenv("ROT_STATS_TAB", "Rotation_Stats")

# Source columns (Rotation_Log)
LOG_TOKEN_COL    = os.getenv("REBUY_LOG_TOKEN_COL", "Token")
LOG_DEC_COL      = os.getenv("REBUY_LOG_DEC_COL", "Decision")
LOG_STATUS_COL   = os.getenv("REBUY_LOG_STATUS_COL", "Status")
# ROI column candidates (Rotation_Log)
LOG_ROI_COLS     = [c.strip() for c in os.getenv("REBUY_LOG_ROI_COLS", "Rebuy ROI%,ROI%,ROI").split(",") if c.strip()]

# Destination columns (Rotation_Stats)
STATS_TOKEN_COL  = os.getenv("ROT_STATS_TOKEN_COL", "Token")
STATS_REBUY_CT   = os.getenv("ROT_STATS_REBUY_CT_COL", "Rebuy Count")
STATS_REBUY_WIN  = os.getenv("ROT_STATS_REBUY_WIN_COL", "Rebuy Win%")
STATS_REBUY_AVG  = os.getenv("ROT_STATS_REBUY_AVG_COL", "Rebuy Avg ROI%")

# Performance / safety
TTL_LOG_S        = int(os.getenv("REBUY_ROI_TTL_LOG_S", "1800"))      # 30 min
TTL_STATS_S      = int(os.getenv("REBUY_ROI_TTL_STATS_S", "600"))     # 10 min
MAX_WRITES       = int(os.getenv("REBUY_ROI_MAX_WRITES", "250"))      # cap per run
JITTER_S         = float(os.getenv("REBUY_ROI_JITTER_S", "2.0"))      # jitter to spread load


def _col_letter(n: int) -> str:
    """1-indexed column number -> Excel letter (A, B, ..., AA)."""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _hmap(header: List[Any]) -> Dict[str, int]:
    """Header list -> {name: index} with stripped string keys."""
    out: Dict[str, int] = {}
    for i, h in enumerate(header or []):
        k = str(h).strip()
        if k and k not in out:
            out[k] = i
    return out


def _pick_first_index(h: Dict[str, int], names: List[str]) -> Optional[int]:
    for n in names:
        if n in h:
            return h[n]
    return None


def _is_rebuy(decision: str, status: str) -> bool:
    d = (decision or "").strip().upper()
    s = (status or "").strip().upper()
    # conservative: count rows that look like rebuy decisions, regardless of fill state
    # (your existing system treats these as "rebuy" performance lines)
    if "REBUY" in d:
        return True
    if "REBUY" in s:
        return True
    return False


def _ensure_col(header: List[Any], h: Dict[str, int], header_writes: List[Dict[str, Any]], name: str) -> int:
    """Ensure column exists in header; stage header write if added."""
    if name in h:
        return h[name]
    header.append(name)
    idx = len(header) - 1
    h[name] = idx
    header_writes.append({"range": f"{_col_letter(idx+1)}1", "values": [[name]]})
    return idx


def _load_rotation_log_rows() -> List[Dict[str, Any]]:
    """
    Prefer DB mirror for Rotation_Log if available; otherwise use Sheets values.
    Returns normalized list[dict] for easier processing.
    """
    # DB-aware path (expects list[dict] already)
    if get_all_records_cached_dbaware:
        try:
            rows = get_all_records_cached_dbaware(LOG_TAB, ttl_s=TTL_LOG_S, logical_stream=f"sheet_mirror:{LOG_TAB}")
            if isinstance(rows, list) and (not rows or isinstance(rows[0], dict)):
                return rows or []
        except Exception:
            pass

    # Sheets fallback: values -> dicts using header
    vals = get_values_cached(LOG_TAB, ttl_s=TTL_LOG_S) or []
    if not vals or not vals[0]:
        return []
    header = [str(x).strip() for x in vals[0]]
    out: List[Dict[str, Any]] = []
    for r in vals[1:]:
        d: Dict[str, Any] = {}
        for i, k in enumerate(header):
            if not k:
                continue
            d[k] = r[i] if i < len(r) else ""
        out.append(d)
    return out


def run_rebuy_roi_tracker() -> None:
    # jitter to avoid synchronized bursts across jobs
    if JITTER_S > 0:
        time.sleep(random.random() * JITTER_S)

    print("🔁 Syncing Rebuy ROI → Rotation_Stats...")

    log_rows = _load_rotation_log_rows()
    if not log_rows:
        print(f"ℹ️ {LOG_TAB} empty; skipping.")
        return

    # Validate required columns exist in log rows
    # (since dicts may be missing keys for some rows, we check presence in any row)
    def _has_any_key(k: str) -> bool:
        return any((k in r and str(r.get(k, "")).strip() != "") for r in log_rows)

    miss = []
    if not _has_any_key(LOG_TOKEN_COL): miss.append(LOG_TOKEN_COL)
    if not _has_any_key(LOG_DEC_COL):   miss.append(LOG_DEC_COL)
    if not _has_any_key(LOG_STATUS_COL): miss.append(LOG_STATUS_COL)
    roi_key = None
    for c in LOG_ROI_COLS:
        if _has_any_key(c):
            roi_key = c
            break
    if roi_key is None:
        miss.append(f"one of {LOG_ROI_COLS}")

    if miss:
        print(f"⚠️ {LOG_TAB} missing columns: {', '.join(miss)}; skipping.")
        return

    # Aggregate per token: sum ROI, count, win count
    agg: Dict[str, Dict[str, float]] = {}
    agg_cnt: Dict[str, int] = {}
    agg_win: Dict[str, int] = {}

    for r in log_rows:
        token = str_or_empty(r.get(LOG_TOKEN_COL, "")).upper()
        if not token:
            continue
        dec = str_or_empty(r.get(LOG_DEC_COL, ""))
        st  = str_or_empty(r.get(LOG_STATUS_COL, ""))
        if not _is_rebuy(dec, st):
            continue

        roi = to_float(r.get(roi_key, ""), default=None)
        if roi is None:
            continue

        agg[token] = agg.get(token, {"sum": 0.0})
        agg[token]["sum"] += float(roi)
        agg_cnt[token] = agg_cnt.get(token, 0) + 1
        if float(roi) > 0:
            agg_win[token] = agg_win.get(token, 0) + 1

    if not agg_cnt:
        print("ℹ️ No rebuy rows to aggregate.")
        return

    # -------- ONE cached read of Rotation_Stats
    stats_vals = get_values_cached(STATS_TAB, ttl_s=TTL_STATS_S) or []
    if not stats_vals or not stats_vals[0]:
        print(f"ℹ️ {STATS_TAB} empty; skipping.")
        return

    header = stats_vals[0]
    h = _hmap(header)

    si_token = h.get(STATS_TOKEN_COL)
    if si_token is None:
        print(f"⚠️ {STATS_TAB} missing required column: {STATS_TOKEN_COL}; skipping.")
        return

    header_writes: List[Dict[str, Any]] = []
    si_ct  = _ensure_col(header, h, header_writes, STATS_REBUY_CT)
    si_win = _ensure_col(header, h, header_writes, STATS_REBUY_WIN)
    si_avg = _ensure_col(header, h, header_writes, STATS_REBUY_AVG)

    # Compute row diffs (no per-row update_cell)
    writes: List[Dict[str, Any]] = []
    touched = 0

    for r_idx, row in enumerate(stats_vals[1:], start=2):
        token = str_or_empty(row[si_token] if si_token < len(row) else "").upper()
        if not token or token not in agg_cnt:
            continue

        cnt = int(agg_cnt[token])
        if cnt <= 0:
            continue
        avg = float(agg[token]["sum"]) / cnt
        winp = 100.0 * float(agg_win.get(token, 0)) / cnt

        # existing values
        cur_ct  = to_float(row[si_ct]  if si_ct  < len(row) else "", default=None)
        cur_win = to_float(row[si_win] if si_win < len(row) else "", default=None)
        cur_avg = to_float(row[si_avg] if si_avg < len(row) else "", default=None)

        # only write if changed meaningfully
        def _need(cur, new) -> bool:
            if cur is None:
                return True
            try:
                return abs(float(cur) - float(new)) > 1e-9
            except Exception:
                return True

        if _need(cur_ct, cnt):
            writes.append({"range": f"{_col_letter(si_ct+1)}{r_idx}", "values": [[cnt]]})
            touched += 1
        if _need(cur_win, winp):
            writes.append({"range": f"{_col_letter(si_win+1)}{r_idx}", "values": [[round(winp, 4)]]})
            touched += 1
        if _need(cur_avg, avg):
            writes.append({"range": f"{_col_letter(si_avg+1)}{r_idx}", "values": [[round(avg, 6)]]})
            touched += 1

        if touched >= MAX_WRITES:
            break

    if not header_writes and not writes:
        print("✅ Rebuy ROI tracker: no changes needed.")
        return

    # Single batched write
    ws = get_ws(STATS_TAB)
    payload: List[Dict[str, Any]] = []
    if header_writes:
        payload.extend(header_writes)
    if writes:
        payload.extend(writes)
    ws_batch_update(ws, payload)
    print(f"✅ Rebuy ROI tracker: wrote {touched} cell(s){' + header' if header_writes else ''}.")
