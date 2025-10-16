#!/usr/bin/env python3
# health_summary.py — Daily Telegram report for NovaTrade Bus
import os, sqlite3, json, time, math, urllib.parse, requests
from datetime import datetime, timezone

DB_PATH   = os.getenv("BUS_TELEMETRY_DB", "bus_telemetry.db")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")         # e.g. "-1001234567890"
TITLE     = os.getenv("HEALTH_TITLE", "☀️ NovaTrade Daily Report")
STALE_MIN = int(os.getenv("HEARTBEAT_STALE_MIN", "60"))  # alert if last HB older than this

def _now_utc():
    return datetime.now(timezone.utc)

def _fmt_ts(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except:
        return str(ts)

def _open():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def _last_heartbeats(con):
    rows = con.execute("""
        SELECT agent, MAX(ts) AS ts, MAX(latency_ms) AS latency_ms
        FROM telemetry_heartbeat
        GROUP BY agent
        ORDER BY agent
    """).fetchall()
    out = []
    for r in rows:
        out.append({"agent": r["agent"], "ts": int(r["ts"] or 0), "latency_ms": int(r["latency_ms"] or 0)})
    return out

def _last_pushes(con):
    # last telemetry_push row per agent
    rows = con.execute("""
        SELECT agent, aggregates_json, MAX(id) AS id
        FROM telemetry_push
        GROUP BY agent
    """).fetchall()
    out = {}
    for r in rows:
        try:
            agg = json.loads(r["aggregates_json"] or "{}")
        except Exception:
            agg = {}
        out[r["agent"]] = agg
    return out

def _compose_message(hbs, pushes):
    now = _now_utc()
    lines = [TITLE, f"_as of {now.strftime('%Y-%m-%d %H:%M UTC')}_", ""]
    warn_count = 0

    # Heartbeats
    lines.append("**Heartbeats**")
    if not hbs:
        lines.append("• (no heartbeats recorded yet)")
    for hb in hbs:
        age_min = max(0, int((now.timestamp() - hb["ts"]) / 60)) if hb["ts"] else 10**9
        stale = " ⚠️" if age_min > STALE_MIN else " ✅"
        if age_min > STALE_MIN: warn_count += 1
        lines.append(f"• {hb['agent']}: last {_fmt_ts(hb['ts'])} ({age_min}m ago){stale}")
    lines.append("")

    # Trades + balances
    lines.append("**24h Trades & Last Balances**")
    if not pushes:
        lines.append("• (no telemetry pushes yet)")
    for agent, agg in pushes.items():
        t24 = agg.get("trades_24h") or {}
        bal = agg.get("last_balances") or {}
        # trades by venue
        tparts = [f"{v}:{c}" for v, c in t24.items()] or ["none"]
        lines.append(f"• {agent} – trades(24h): " + ", ".join(tparts))
        # balances (collapse to per-venue USD-ish snapshot if available)
        if bal:
            for venue, amap in bal.items():
                # Show up to a few headline assets
                interesting = []
                for asset, free in (amap or {}).items():
                    if asset.upper() in ("USDT","USDC","BTC","XBT","ETH"):
                        interesting.append(f"{asset}:{free:.6f}")
                if interesting:
                    lines.append(f"   ↳ {venue}: " + ", ".join(interesting))
        else:
            lines.append("   ↳ (no balances captured)")
    lines.append("")

    # Footer
    if warn_count:
        lines.append(f"⚠️ {warn_count} warning(s): at least one agent heartbeat is stale (> {STALE_MIN} min).")
    else:
        lines.append("All agents fresh. 🟢")

    # MarkdownV2 safe-ish (Telegram supports Markdown, but we keep simple)
    msg = "\n".join(lines)
    return msg

def _send_telegram(text: str):
    if not (BOT_TOKEN and CHAT_ID):
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    r = requests.post(url, data=data, timeout=15)
    ok = r.status_code == 200
    return ok, (r.json() if ok else r.text)

def main():
    try:
        con = _open()
    except Exception as e:
        print(f"health_summary: DB open error ({DB_PATH}): {e}")
        return 2

    try:
        hbs = _last_heartbeats(con)
        pushes = _last_pushes(con)
        msg = _compose_message(hbs, pushes)
        ok, resp = _send_telegram(msg)
        if not ok:
            print("health_summary: Telegram error:", resp)
            return 3
        print("health_summary: sent")
        return 0
    except Exception as e:
        print("health_summary error:", e)
        return 4

if __name__ == "__main__":
    raise SystemExit(main())
