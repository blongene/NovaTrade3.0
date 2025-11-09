
# policy_bias_engine.py — Phase 10 scaffolding
# Builds a per-token Policy Bias from Rotation_Memory, Rotation_Stats and recent Policy_Log.
# Writes to Policy_Bias tab and exposes get_bias_map() for routers/policy to consult.
# Safe under missing tabs; never raises.
import os, math, time
from datetime import datetime, timedelta

try:
    from utils import get_gspread_client, warn, info
except Exception:
    def warn(x): print("[policy_bias] WARN:", x)
    def info(x): print("[policy_bias] INFO:", x)
    get_gspread_client = None

SHEET_URL = os.getenv("SHEET_URL", "")
BIAS_WS   = os.getenv("POLICY_BIAS_WS", "Policy_Bias")
MEMORY_WS = os.getenv("ROTATION_MEMORY_WS", "Rotation_Memory")
STATS_WS  = os.getenv("ROTATION_STATS_WS", "Rotation_Stats")
PLOG_WS   = os.getenv("POLICY_LOG_WS", "Policy_Log")

# bounds for bias factor (applied to notional/weight/etc. by driver)
MIN_FACTOR = float(os.getenv("POLICY_BIAS_MIN", "0.75"))
MAX_FACTOR = float(os.getenv("POLICY_BIAS_MAX", "1.25"))

LOOKBACK_DAYS = int(os.getenv("POLICY_BIAS_LOOKBACK_DAYS", "30"))

def _open():
    gc = get_gspread_client()
    return gc.open_by_url(SHEET_URL)

def _get(ws_name):
    try:
        return _open().worksheet(ws_name).get_all_records()
    except Exception as e:
        warn(f"read failed {ws_name}: {e}")
        return []

def _ensure_ws(name, headers):
    try:
        sh = _open()
        try:
            ws = sh.worksheet(name)
            vals = ws.get_all_values()
            if not vals:
                ws.append_row(headers, value_input_option="USER_ENTERED")
            return ws
        except Exception:
            ws = sh.add_worksheet(title=name, rows=2000, cols=max(10, len(headers)+2))
            ws.append_row(headers, value_input_option="USER_ENTERED")
            return ws
    except Exception as e:
        warn(f"ensure_ws {name} failed: {e}")
        return None

def _safe_float(x):
    try:
        s = str(x).replace("%","").replace(",","").strip()
        if s == "" or s.upper() == "N/A": return None
        return float(s)
    except Exception:
        return None

def _z(v, mu, sigma):
    if sigma is None or sigma <= 1e-9 or v is None: return 0.0
    return (v - mu) / sigma

def _mean_std(vals):
    xs = [v for v in vals if v is not None]
    if not xs: return (None, None)
    m = sum(xs)/len(xs)
    var = sum((x-m)**2 for x in xs)/max(1, len(xs)-1)
    return (m, var**0.5)

def _parse_ts(s):
    if not s: return None
    s = str(s).replace("Z","").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

def _policy_ok_rate(rows, lookback_days=30):
    if not rows: return (None, 0)
    cutoff = datetime.utcnow() - timedelta(days=lookback_days)
    oks = total = 0
    for r in rows:
        ts = _parse_ts(r.get("Timestamp") or r.get("Time") or "")
        if ts and ts < cutoff: continue
        total += 1
        ok = str(r.get("OK") or r.get("ok") or "").strip().upper() in ("TRUE","1","YES","OK")
        oks += 1 if ok else 0
    if total == 0: return (None, 0)
    return (oks/total, total)

def _collect():
    mem   = _get(MEMORY_WS)
    stats = _get(STATS_WS)
    plog  = _get(PLOG_WS)

    # map token -> memory weighted score
    mem_map = {}
    for r in mem:
        t = str(r.get("Token","")).strip().upper()
        s = _safe_float(r.get("Weighted_Score"))
        if t and s is not None:
            mem_map[t] = s

    # map token -> recent ROI proxy (Follow-up ROI or ROI_7d if present)
    roi_map = {}
    for r in stats:
        t = str(r.get("Token","")).strip().upper()
        if not t: continue
        roi = None
        for k in ("ROI_7d","ROI 7d","ROI7d","Follow-up ROI"):
            roi = _safe_float(r.get(k))
            if roi is not None: break
        if roi is not None:
            roi_map[t] = roi

    # global stats for normalization
    mem_vals = list(mem_map.values())
    roi_vals = list(roi_map.values())

    m_mu, m_sd = _mean_std(mem_vals)
    r_mu, r_sd = _mean_std(roi_vals)

    # policy ok-rate global (acts as confidence anchor)
    ok_rate, sample = _policy_ok_rate(plog, LOOKBACK_DAYS)
    return mem_map, roi_map, (m_mu, m_sd), (r_mu, r_sd), (ok_rate, sample)

def _to_factor(score):
    # score ~ N(0,1). Map z-score into [MIN_FACTOR, MAX_FACTOR] via sigmoid-ish curve.
    lo, hi = MIN_FACTOR, MAX_FACTOR
    # squashing: 0.5 * tanh(score/2) + 0.5 => [0,1]
    s = 0.5 * (math.tanh(score/2.0) + 1.0)
    return lo + s * (hi - lo)

def run_policy_bias_builder():
    if not SHEET_URL:
        warn("SHEET_URL missing; abort.")
        return

    mem_map, roi_map, mem_norm, roi_norm, ok_stats = _collect()
    m_mu, m_sd = mem_norm
    r_mu, r_sd = roi_norm
    ok_rate, ok_samples = ok_stats

    tokens = sorted(set(list(mem_map.keys()) + list(roi_map.keys())))
    rows = []
    for t in tokens:
        m = mem_map.get(t)
        r = roi_map.get(t)
        mz = _z(m, m_mu, m_sd)
        rz = _z(r, r_mu, r_sd)
        # composite z with weights: memory dominates
        comp = 0.65*mz + 0.35*rz
        factor = _to_factor(comp)
        conf = 0.0
        # confidence: upweight with available signals + ok samples
        sigs = (1 if m is not None else 0) + (1 if r is not None else 0)
        conf = min(1.0, 0.3*sigs + 0.7*min(1.0, (ok_samples or 0)/25.0))
        rows.append([t, f"{factor:.3f}", f"{conf:.2f}", f"{m if m is not None else ''}", f"{r if r is not None else ''}", f"{mz:.2f}", f"{rz:.2f}"])

    ws = _ensure_ws(BIAS_WS, ["Token","Bias_Factor","Confidence","Weighted_Score","ROI_Proxy","Z_Memory","Z_ROI"])
    if ws:
        try:
            ws.clear()
            ws.append_row(["Token","Bias_Factor","Confidence","Weighted_Score","ROI_Proxy","Z_Memory","Z_ROI"], value_input_option="USER_ENTERED")
            if rows:
                ws.append_rows(rows, value_input_option="USER_ENTERED")
            info(f"Policy_Bias written for {len(rows)} tokens.")
        except Exception as e:
            warn(f"Policy_Bias write failed: {e}")

def get_bias_map():
    # read Policy_Bias and return {TOKEN: (factor, confidence)}
    try:
        rows = _get(BIAS_WS)
        out = {}
        for r in rows:
            t = str(r.get("Token","")).strip().upper()
            f = _safe_float(r.get("Bias_Factor"))
            c = _safe_float(r.get("Confidence"))
            if t and f is not None:
                out[t] = (f, c if c is not None else 0.0)
        return out
    except Exception:
        return {}

if __name__ == "__main__":
    run_policy_bias_builder()
