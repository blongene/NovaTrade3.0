# performance_dashboard.py
# Phase 8A – Analytics / Performance Dashboard 2.0
# Reads Rotation_Stats, Rotation_Log, Vault_Intelligence, Rotation_Memory, Trade_Log
# Writes Performance_Dashboard and sends a Telegram "NovaScore Daily" summary.

import os
import math
from collections import defaultdict
from statistics import mean, pstdev
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")
DASHBOARD_WS = os.getenv("PERF_DASHBOARD_WS", "Performance_Dashboard")
MEMORY_WS = os.getenv("ROTATION_MEMORY_WS", "Rotation_Memory")
VAULT_INTEL_WS = os.getenv("VAULT_INTELLIGENCE_WS", "Vault_Intelligence")

# Optional Telegram env (works with your existing sender if present)
BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# -------- Sheet helpers --------
def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
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
    if x is None:
        return None
    s = str(x).strip()
    # common bad values
    if s == "" or s.upper() == "N/A":
        return None
    # remove percent sign
    s = s.replace("%", "").strip()
    # strip phrases like "0d since vote"
    if "since" in s.lower() or "day" in s.lower() or "d " in s.lower():
        # nothing numeric here worth trusting
        return None
    # allow e.g. "1,234.56"
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None

def _pct_str(x):
    return "" if x is None else f"{x:.2f}%"

def _ratio(a, b):
    try:
        if a is None or b is None or b == 0:
            return None
        return a / b
    except Exception:
        return None

# -------- Telegram sender (adapts to your existing utils if available) --------
def _send_telegram(msg: str):
    """
    Tries your project sender first; falls back to direct Bot API only if token+chat present.
    If neither available, just prints.
    """
    try:
        # Try your existing helper
        try:
            from send_telegram import send_rotation_alert  # your project sender
            send_rotation_alert("NovaScore", 0, msg, 0)    # light overload as a generic sender
            return
        except Exception:
            pass

        try:
            from utils import send_telegram_message
            send_telegram_message(msg)
            return
        except Exception:
            pass

        # Bare fallback (not required if your sender exists)
        if BOT_TOKEN and TELEGRAM_CHAT_ID:
            import requests
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                timeout=10,
            )
            return
    except Exception:
        pass
    print(f"[Telegram] {msg}")

# -------- Analytics --------
def _compute_sharpe(roi_series_pct):
    """
    Approx Sharpe-like metric from a list of ROI percentages (e.g., multiple entries from Rotation_Stats).
    Treats series as “periodic returns” in % units; risk-free ~0; population stddev.
    Returns None if insufficient data.
    """
    vals = [v for v in roi_series_pct if v is not None]
    if len(vals) < 3:
        return None
    mu = mean(vals)
    sigma = pstdev(vals)
    if sigma == 0:
        return None
    # Scale to a 30-day-ish horizon if these are near-daily/weekly; this is heuristic.
    return mu / sigma

def _collect_token_sets():
    rotation_stats = _get("Rotation_Stats")
    rotation_log   = _get("Rotation_Log")
    vault_intel    = _get(VAULT_INTEL_WS or "Vault Intelligence")
    trade_log      = _get("Trade_Log")
    rotation_mem   = _get(MEMORY_WS)

    tokens = set()
    for rows in (rotation_stats, rotation_log, vault_intel, trade_log, rotation_mem):
        for r in rows:
            t = str(r.get("Token", "")).strip().upper()
            if t:
                tokens.add(t)

    return {
        "rotation_stats": rotation_stats,
        "rotation_log": rotation_log,
        "vault_intel": vault_intel,
        "trade_log": trade_log,
        "rotation_mem": rotation_mem,
        "tokens": tokens,
    }

def _aggregate_metrics():
    data = _collect_token_sets()
    RS = data["rotation_stats"]
    RL = data["rotation_log"]
    VI = data["vault_intel"]
    TL = data["trade_log"]
    RM = data["rotation_mem"]
    tokens = data["tokens"]

    # Index helpers
    mem_index = {str(r.get("Token","")).strip().upper(): r for r in RM}
    vi_index  = {str(r.get("Token","")).strip().upper(): r for r in VI}

    # ROI series per token from Rotation_Stats (use Follow-up ROI if present; else Initial)
    roi7_map  = defaultdict(list)
    roi30_map = defaultdict(list)

    # If Rotation_Stats already has 7d/30d columns in your version, prefer those; else derive best-effort
    for r in RS:
        t = str(r.get("Token","")).strip().upper()
        if not t:
            continue
        roi_follow = _safe_float(r.get("Follow-up ROI"))
        roi_init   = _safe_float(r.get("Initial ROI"))
        # Best-effort series for Sharpe: use whatever is numeric
        base = roi_follow if roi_follow is not None else roi_init
        if base is not None:
            # Treat as a general ROI observation; it feeds Sharpe
            roi7_map[t].append(base)
            roi30_map[t].append(base)

    # Allocation (%) and health from Rotation_Log
    alloc_map = {}
    for r in RL:
        t = str(r.get("Token","")).strip().upper()
        if not t:
            continue
        alloc = _safe_float(r.get("Allocation (%)"))
        if alloc is not None:
            alloc_map[t] = alloc

    # Win/Loss from Rotation_Memory
    win_rate = {}
    for t, r in mem_index.items():
        wins  = _safe_float(r.get("Wins"))
        losses = _safe_float(r.get("Losses"))
        wins = wins if wins is not None else 0
        losses = losses if losses is not None else 0
        total = (wins or 0) + (losses or 0)
        wr = (wins / total * 100.0) if total > 0 else None
        win_rate[t] = wr

    # Liquidity / Memory score from Vault_Intelligence (if present)
    liq_map = {}
    mem_score_map = {}
    for t, r in vi_index.items():
        liq = _safe_float(r.get("liquidity_usd"))
        ms  = _safe_float(r.get("memory_score"))
        if liq is not None:
            liq_map[t] = liq
        if ms is not None:
            mem_score_map[t] = ms

    # Compose rows
    rows = []
    for t in sorted(tokens):
        r7_list  = roi7_map.get(t, [])
        r30_list = roi30_map.get(t, [])
        sharpe   = _compute_sharpe(r7_list)

        # Aggregate “typical” ROI levels (median-like via mean for now)
        roi7   = mean(r7_list) if r7_list else None
        roi30  = mean(r30_list) if r30_list else None
        alloc  = alloc_map.get(t)
        wr     = win_rate.get(t)
        liq    = liq_map.get(t)
        mscore = mem_score_map.get(t)

        # NovaScore (0–100):  (weights can be tuned later)
        #   35% ROI30, 25% ROI7, 20% Sharpe scaled, 15% WinRate, 5% MemoryScore
        # Sharpe scale: ~[-2..+3] -> clamp to [-2..+3], map to 0..100 bucket
        def clamp(x, lo, hi):
            return lo if x < lo else hi if x > hi else x

        sharpe_scaled = None
        if sharpe is not None:
            sh = clamp(sharpe, -2.0, 3.0)
            # map [-2..+3] to [0..100]
            sharpe_scaled = (sh + 2.0) / 5.0 * 100.0

        # Normalize parts into 0..100; ROI are already in %, win rate is %, memscore ~0..1
        ns = 0.0
        parts = 0.0

        if roi30 is not None:
            ns += 0.35 * clamp(roi30, -100.0, 300.0)  # guard rails
            parts += 0.35
        if roi7 is not None:
            ns += 0.25 * clamp(roi7, -100.0, 300.0)
            parts += 0.25
        if sharpe_scaled is not None:
            ns += 0.20 * sharpe_scaled
            parts += 0.20
        if wr is not None:
            ns += 0.15 * clamp(wr, 0.0, 100.0)
            parts += 0.15
        if mscore is not None:
            # assume 0..1; convert to 0..100
            ns += 0.05 * clamp(mscore * 100.0, 0.0, 100.0)
            parts += 0.05

        novasc = ns / parts if parts > 0 else None

        rows.append({
            "Token": t,
            "ROI_7d": roi7,
            "ROI_30d": roi30,
            "Sharpe": sharpe,
            "Win_Rate_%": wr,
            "Allocation_%": alloc,
            "Liquidity_USD": liq,
            "Memory_Score": mscore,
            "NovaScore": novasc
        })

    return rows

def _write_dashboard(rows):
    sh = _open_sheet()
    ws = _ensure_ws(sh, DASHBOARD_WS, rows=max(1000, len(rows)+10), cols=12)
    headers = [
        "Timestamp",
        "Token",
        "ROI_7d (%)",
        "ROI_30d (%)",
        "Sharpe",
        "Win Rate (%)",
        "Allocation (%)",
        "Liquidity (USD)",
        "Memory Score",
        "NovaScore"
    ]
    ws.append_row(headers)

    body = []
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    for r in rows:
        body.append([
            now,
            r["Token"],
            "" if r["ROI_7d"] is None else round(r["ROI_7d"], 2),
            "" if r["ROI_30d"] is None else round(r["ROI_30d"], 2),
            "" if r["Sharpe"] is None else round(r["Sharpe"], 3),
            "" if r["Win_Rate_%"] is None else round(r["Win_Rate_%"], 2),
            "" if r["Allocation_%"] is None else round(r["Allocation_%"], 2),
            "" if r["Liquidity_USD"] is None else round(r["Liquidity_USD"], 2),
            "" if r["Memory_Score"] is None else round(r["Memory_Score"], 3),
            "" if r["NovaScore"] is None else round(r["NovaScore"], 2),
        ])
    if body:
        ws.append_rows(body)

def _send_daily_summary(rows):
    if not rows:
        _send_telegram("🧭 NovaScore Daily: no tokens to summarize.")
        return

    # Top 3 by NovaScore
    ranked = [r for r in rows if r["NovaScore"] is not None]
    ranked.sort(key=lambda x: x["NovaScore"], reverse=True)
    top = ranked[:3]

    # Portfolio snapshot (approx from allocations that are present)
    allocs = [r["Allocation_%"] for r in rows if r["Allocation_%"] is not None]
    port_alloc = sum(allocs) if allocs else None

    lines = ["*🧭 NovaScore Daily*"]
    if port_alloc is not None:
        lines.append(f"Portfolio coverage: {port_alloc:.1f}% (from Rotation_Log allocations)")

    if top:
        lines.append("_Top by NovaScore:_")
        for i, r in enumerate(top, 1):
            s7 = "—"
            if r["ROI_7d"] is not None:
                s7 = f"{r['ROI_7d']:.2f}%"
            s30 = "—"
            if r["ROI_30d"] is not None:
                s30 = f"{r['ROI_30d']:.2f}%"
            sh = "—" if r["Sharpe"] is None else f"{r['Sharpe']:.2f}"
            wr = "—" if r["Win_Rate_%"] is None else f"{r['Win_Rate_%']:.1f}%"
            ns = f"{r['NovaScore']:.1f}"
            lines.append(f"*{i}.* {r['Token']} — NovaScore {ns} | 7d {s7} | 30d {s30} | Sharpe {sh} | Win {wr}")
    else:
        lines.append("_No tokens have a computed NovaScore yet._")

    _send_telegram("\n".join(lines))

# -------- Public entrypoint --------
def run_performance_dashboard():
    try:
        rows = _aggregate_metrics()
        _write_dashboard(rows)
        _send_daily_summary(rows)
        print("✅ Performance_Dashboard updated and NovaScore Daily sent.")
    except Exception as e:
        print(f"❌ performance_dashboard failed: {e}")
