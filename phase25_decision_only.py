"""phase25_decision_only.py — Phase 25A (Bus)

Decision-only mode: compute and log "what we would do" without enqueueing trades.

Outputs
- Appends a structured decision record to Policy_Log via policy_logger.log_decision()
- Optional Telegram summary (deduped) if enabled in DB_READ_JSON

Config (DB_READ_JSON)
{
  "phase25": {
    "enabled": 1,
    "decision_only": 1,
    "interval_sec": 900,
    "notify": 1,
    "agent_id": "edge-primary,edge-nl1"
  }
}

Safety
- Never enqueues commands in Phase 25A.
- Tolerant of missing DB/Sheets; will no-op with a single INFO log.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# -----------------------
# Row selection helpers
# -----------------------

def _parse_ts_any(v: Any) -> Optional[float]:
    """Parse a timestamp-ish value into epoch seconds (best-effort)."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        # already epoch-ish
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    # Common formats in NovaTrade:
    # - "YYYY-MM-DD HH:MM:SS"
    # - "YYYY-MM-DDTHH:MM:SS"
    # - "YYYY-MM-DD" (date only)
    s_norm = s.replace("T", " ")
    try:
        # handle date-only
        if len(s_norm) == 10 and s_norm[4] == "-" and s_norm[7] == "-":
            dt = datetime.strptime(s_norm, "%Y-%m-%d")
        else:
            # trim fractional seconds if present
            if "." in s_norm:
                s_norm = s_norm.split(".", 1)[0]
            dt = datetime.strptime(s_norm, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


def _pick_latest_row(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pick the most recent row by Timestamp/ts/Date if possible; else fallback to last."""
    if not rows:
        out = {}
    best_row = None
    best_ts = None
    for r in rows:
        if not isinstance(r, dict):
            continue
        # common keys (Wallet_Monitor, Trade_Log, Unified_Snapshot)
        for k in ("Timestamp", "ts", "TS", "timestamp", "Date", "date"):
            if k in r:
                t = _parse_ts_any(r.get(k))
                if t is None:
                    continue
                if best_ts is None or t > best_ts:
                    best_ts = t
                    best_row = r
                break
    return best_row if best_row is not None else rows[-1]
_LOG_ONCE = set()

_LOCK = threading.Lock()  # protects cycle execution if scheduler overlaps


def _log_once(msg: str) -> None:
    if msg in _LOG_ONCE:
        return
    _LOG_ONCE.add(msg)
    try:
        import logging
        logging.getLogger("bus").info(msg)
    except Exception:
        print(msg)


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _load_db_read_json() -> Dict[str, Any]:
    raw = (os.getenv("DB_READ_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _cfg() -> Dict[str, Any]:
    cfg = _load_db_read_json()
    p = cfg.get("phase25") or {}
    return p if isinstance(p, dict) else {}


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _get_agent_id() -> str:
    c = _cfg()
    a = (c.get("agent_id") or "").strip()
    return a or "edge-primary,edge-nl1"


def _interval_sec() -> int:
    c = _cfg()
    try:
        v = int(c.get("interval_sec") or 900)
        return max(60, min(v, 6 * 3600))
    except Exception:
        return 900


def enabled() -> bool:
    c = _cfg()
    if "enabled" in c:
        return _truthy(c.get("enabled"))
    # default OFF unless explicitly enabled (safer for upgrades)
    return False


def decision_only() -> bool:
    c = _cfg()
    if "decision_only" in c:
        return _truthy(c.get("decision_only"))
    return True


def notify() -> bool:
    c = _cfg()
    return _truthy(c.get("notify", 0))


def _cloud_hold_active() -> bool:
    # Prefer existing helper if present (wsgi defines _cloud_hold_active), else env.
    try:
        v = os.getenv("CLOUD_HOLD", "0").strip().lower()
        return v in {"1", "true", "yes", "y", "on"}
    except Exception:
        return False


def _edge_authority(agent_id: str) -> Dict[str, Any]:
    try:
        from edge_authority import evaluate_agent  # Phase 24C
        ok, reason, age = evaluate_agent(agent_id)
        return {"trusted": bool(ok), "reason": reason, "age_sec": age}
    except Exception as e:
        return {"trusted": False, "reason": f"edge_authority_error:{e.__class__.__name__}", "age_sec": None}


def _read_hot_path(tab: str, limit: int = 200) -> Dict[str, Any]:
    """Read recent rows for a tab using DB-first adapter with Sheets fallback."""
    try:
        from db_read_adapter import get_records_prefer_db  # Phase 22B/23/12
        from utils import get_records_cached  # Sheets fallback (cached)
        rows = get_records_prefer_db(
            tab,
            f"sheet_mirror:{tab}",
            sheets_fallback_fn=lambda *args, **kwargs: get_records_cached(tab),
        )
        if not rows:
            return {"tab": tab, "rows": 0}
        # keep it lightweight
        sample = _pick_latest_row(rows)
        return {"tab": tab, "rows": len(rows), "last": sample}
    except Exception as e:
        return {"tab": tab, "error": f"{e.__class__.__name__}:{e}"}


def build_decision() -> Dict[str, Any]:
    """
    Phase 25A decision record builder.

    Goal: produce *human-readable*, low-noise "would do" records without enqueueing or writing to Sheets.
    """
    agent = _get_agent_id()
    auth = _edge_authority(agent)
    cloud_hold = _cloud_hold_active()

    wallet = _read_hot_path("Wallet_Monitor", limit=200)
    trades = _read_hot_path("Trade_Log", limit=200)

    # Vault Intelligence signals (read-only)
    signals = []
    sig_err = None
    try:
        from phase25_vault_signals import compute_vault_signals  # type: ignore

        signals, sig_err = compute_vault_signals(max_items=25)
    except Exception as e:
        sig_err = f"{e.__class__.__name__}:{e}"

    # Decision framing (Phase 25A: NO COMMANDS)
    ok = (not cloud_hold) and auth.get("trusted", False)

    # Recommendation is informational, not an action.
    if not ok:
        recommendation = "HOLD"
    else:
        if any(s.get("type") == "SELL_CANDIDATE" for s in signals):
            recommendation = "WOULD_SELL"
        elif any(s.get("type") == "REBUY_CANDIDATE" for s in signals):
            recommendation = "WOULD_REBUY"
        else:
            recommendation = "NOOP"

    reasons = []
    if cloud_hold:
        reasons.append("CLOUD_HOLD=1")
    if not auth.get("trusted", False):
        reasons.append("EDGE_AUTHORITY=untrusted")
    if sig_err:
        reasons.append(f"signals_error={sig_err}")

    # Lightweight summary for human scanning (kept short)
    summary = {
        "sell": [f'{s.get("token")} ({int(float(s.get("confidence") or 0)*100)}%)' for s in signals if s.get("type")=="SELL_CANDIDATE"][:5],
        "rebuy": [f'{s.get("token")} ({int(float(s.get("confidence") or 0)*100)}%)' for s in signals if s.get("type")=="REBUY_CANDIDATE"][:5],
        "watch": [f'{s.get("token")} ({int(float(s.get("confidence") or 0)*100)}%)' for s in signals if s.get("type")=="WATCH"][:5],
    }
    summary["noop"] = (recommendation == "NOOP")

    decision_id = os.urandom(8).hex()

    out = {
        "ok": bool(ok),
        "decision_id": decision_id,
        "ts": _now_ts(),
        "phase": "25A",
        "mode": "decision_only" if decision_only() else "planning_only",
        "agent_id": agent,
        "recommendation": recommendation,
        "reasons": reasons,
        "summary": summary,
        "signals": signals,  # full list (still small)
        "inputs": {
            "edge_authority": auth,
            "cloud_hold": cloud_hold,
            "wallet_monitor": wallet,
            "trade_log": trades,
        },
    }

    # Low-noise diagnostics (only present when relevant)
    if sig_err:
        out["signals_error"] = str(sig_err)

    # Always include a tiny meta breadcrumb for debugging
    out["signals_meta"] = {"count": int(len(signals or [])), "has_error": bool(sig_err)}

    return out


def log_decision(decision: Dict[str, Any]) -> None:
    """Log to Policy_Log using existing logger (handles Sheets + local JSONL)."""
    try:
        from policy_logger import log_decision as _log_policy_decision
    except Exception:
        _log_once("Phase25A: policy_logger missing; skipping decision log")
        return

    intent = {
        "token": "",
        "action": "DECISION",
        "amount_usd": 0,
        "venue": "",
        "quote": "",
        "notes": f"decision_id={decision.get('decision_id')}",
        "source": "phase25_decision_only",
    }
    try:
        _log_policy_decision(decision, intent, when=decision.get("ts"))
    except Exception as e:
        _log_once(f"Phase25A: log_decision failed: {e.__class__.__name__}")


def notify_telegram(decision: Dict[str, Any]) -> None:
    if not notify():
        return
    try:
        from telegram_summaries import send_telegram  # de-duped sender
    except Exception:
        try:
            from nova_trigger import nova_trigger as send_telegram
        except Exception:
            return

    rec = decision.get("recommendation")
    ok = decision.get("ok")
    agent = decision.get("agent_id")
    reasons = ", ".join(decision.get("reasons") or [])
    age = (decision.get("inputs") or {}).get("edge_authority", {}).get("age_sec")
    msg = f"🧭 Phase25A Decision: {rec} ok={ok} agent={agent} age={age}s reasons={reasons}"
    try:
        send_telegram(msg)
    except Exception:
        pass


def run_phase25_decision_cycle() -> Dict[str, Any]:
    """Safe to call from scheduler. Returns the decision dict."""
    if not enabled():
        _log_once("Phase25A: disabled (set DB_READ_JSON.phase25.enabled=1 to enable)")
        return {"ok": False, "skipped": True, "reason": "disabled"}

    with _LOCK:
        decision = build_decision()
        log_decision(decision)
        notify_telegram(decision)
        return decision


_thread_started = False


def start_phase25_background_loop() -> None:
    global _thread_started
    if _thread_started:
        return
    if not enabled():
        return

    _thread_started = True
    interval = _interval_sec()

    def _loop():
        _log_once(f"🧭 Phase25A background loop started (interval={interval}s)")
        while True:
            try:
                run_phase25_decision_cycle()
            except Exception:
                pass
            time.sleep(interval)

    threading.Thread(target=_loop, name="phase25-decision-only", daemon=True).start()


if __name__ == "__main__":
    out = run_phase25_decision_cycle()
    print(_safe_json(out))
