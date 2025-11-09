
# Phase 9A — Rotation Feedback Enhancer
# Purpose: fuse ROI_Review_Log feedback into Rotation_Memory with weighted learning.
# Safe to run repeatedly; idempotent per (Token).
#
# Inputs:
#   - ROI_Review_Log: columns (Token, Date, Feedback, Notes, ROI_At_Ping [%], ...)
#   - Rotation_Stats:  columns (Token, Initial ROI, Follow-up ROI, Days Held, ...)
#
# Output (tab: Rotation_Memory):
#   Token | Wins | Losses | Win_Rate_% | Weighted_Score | Last_Update | Notes
#
# Weighted_Score (0..100) combines:
#   • Feedback signal (+1 for YES-again, -1 for NO-again, 0 otherwise)
#   • ROI magnitude at ping (scaled 0..1 with soft clamp)
#   • Recency bonus (<= 30 days)
#   • Sample size stability (sqrt-like dampener)
#
# Env:
#   SHEET_URL (required)
#   ROTATION_MEMORY_WS (optional; default 'Rotation_Memory')
#
import os
from datetime import datetime
import math
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")
MEMORY_WS = os.getenv("ROTATION_MEMORY_WS", "Rotation_Memory")

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SHEET_URL)

def _get(ws_name):
    try:
        return _open_sheet().worksheet(ws_name).get_all_records()
    except Exception:
        return []

def _ensure_ws(sheet, name, rows=1000, cols=20):
    try:
        ws = sheet.worksheet(name)
        ws.clear()
        return ws
    except gspread.exceptions.WorksheetNotFound:
        return sheet.add_worksheet(title=name, rows=rows, cols=cols)

def _safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip().replace("%","").replace(",","")
        if s == "" or s.upper() == "N/A":
            return None
        return float(s)
    except Exception:
        return None

def _sgn_feedback(feedback: str) -> int:
    if not feedback:
        return 0
    f = str(feedback).strip().lower()
    if f in ("yes", "y", "yes again", "yes-again", "again yes", "re-buy"):
        return +1
    if f in ("no", "n", "no again", "no-again", "never again", "sell"):
        return -1
    return 0

def _clamp(v, lo, hi):
    return lo if v < lo else hi if v > hi else v

def _recency_boost(days_ago: float) -> float:
    # 30d half-life style curve → [0..1]
    if days_ago is None:
        return 0.5
    return _clamp(1.0 - (days_ago / 60.0), 0.0, 1.0)

def _age_days(iso_or_str):
    if not iso_or_str:
        return None
    s = str(iso_or_str).strip().replace("Z","")
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return (datetime.utcnow() - dt).days
        except Exception:
            continue
    return None

def run_rotation_feedback_enhancer():
    if not SHEET_URL:
        print("[Phase9A] SHEET_URL missing; aborting.")
        return

    sh = _open_sheet()

    roi_reviews = _get("ROI_Review_Log")
    rot_stats   = _get("Rotation_Stats")

    # Build per-token aggregates
    agg = {}
    # Seed with existing wins/losses if Rotation_Memory already has data (so we don't erase history)
    try:
        mem_rows = sh.worksheet(MEMORY_WS).get_all_records()
    except Exception:
        mem_rows = []

    for r in mem_rows:
        t = str(r.get("Token","")).strip().upper()
        if not t:
            continue
        wins0   = _safe_float(r.get("Wins")) or 0.0
        losses0 = _safe_float(r.get("Losses")) or 0.0
        agg[t] = {"wins": wins0, "losses": losses0, "notes": []}

    # ROI at ping lookup (best-effort from Rotation_Stats)
    last_roi = {}
    for r in rot_stats:
        t = str(r.get("Token","")).strip().upper()
        if not t:
            continue
        roi_f = _safe_float(r.get("Follow-up ROI"))
        roi_i = _safe_float(r.get("Initial ROI"))
        last_roi[t] = roi_f if roi_f is not None else roi_i

    # Fold reviews
    for r in roi_reviews:
        t = str(r.get("Token","")).strip().upper()
        if not t:
            continue
        sgn = _sgn_feedback(r.get("Feedback") or r.get("Re-Vote") or r.get("ReVote"))
        if t not in agg:
            agg[t] = {"wins": 0.0, "losses": 0.0, "notes": []}
        if sgn > 0:
            agg[t]["wins"] += 1.0
        elif sgn < 0:
            agg[t]["losses"] += 1.0
        # Collect note
        note = str(r.get("Notes","")).strip()
        if note:
            agg[t]["notes"].append(note)

    # Compute outputs
    rows_out = []
    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for t, v in sorted(agg.items()):
        w = float(v["wins"])
        l = float(v["losses"])
        n = w + l

        wr = (w / n * 100.0) if n > 0 else None

        roi = last_roi.get(t)
        roi_norm = 0.0
        if roi is not None:
            roi_norm = _clamp((roi + 100.0) / 200.0, 0.0, 1.0)  # -100..+100 → 0..1

        # Recency from ROI_Review_Log latest date for token
        rec_days = None
        for r in reversed(roi_reviews):
            if str(r.get("Token","")).strip().upper() == t:
                rec_days = _age_days(r.get("Date") or r.get("Timestamp"))
                break
        rec = _recency_boost(rec_days)

        # Stability (sample size) dampener: sqrt-like
        stability = math.sqrt(max(n, 1.0)) / 5.0  # ~0.2 for 1 sample → approaches 1 with 25+ samples
        stability = _clamp(stability, 0.2, 1.0)

        # Weighted_Score (0..100)
        # 50% win-rate, 25% ROI context, 15% recency, 10% stability
        score = 0.0
        parts = 0.0
        if wr is not None:
            score += 0.50 * _clamp(wr, 0.0, 100.0)
            parts += 0.50
        score += 0.25 * (roi_norm * 100.0); parts += 0.25
        score += 0.15 * (rec * 100.0); parts += 0.15
        score += 0.10 * (stability * 100.0); parts += 0.10
        weighted_score = score / parts if parts > 0 else None

        rows_out.append([
            t,
            int(w),
            int(l),
            "" if wr is None else round(wr, 2),
            "" if weighted_score is None else round(weighted_score, 2),
            now_iso,
            "; ".join(v["notes"])[:300]
        ])

    # Write to sheet
    ws = _ensure_ws(sh, MEMORY_WS, rows=max(1000, len(rows_out)+10), cols=12)
    ws.append_row(["Token","Wins","Losses","Win_Rate_%","Weighted_Score","Last_Update","Notes"])
    if rows_out:
        ws.append_rows(rows_out)

    print(f"[Phase9A] Rotation_Memory updated for {len(rows_out)} token(s).")

if __name__ == "__main__":
    run_rotation_feedback_enhancer()
