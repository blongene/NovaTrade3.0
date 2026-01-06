"""phase25_planning_only.py — Phase 25B (Bus)

Planning-only mode: produce a "plan object" (what commands we WOULD enqueue), but do not enqueue.

Inputs
- Reuses Phase 25A decision builder (phase25_decision_only.build_decision)

Outputs
- Logs a PLAN record to Policy_Log via policy_logger.log_decision()
- Optional write to Postgres table nova_plans (best-effort; safe to skip)
- Optional Telegram summary (deduped)

Config (DB_READ_JSON)
{
  "phase25": {
    "enabled": 1,
    "decision_only": 1,          # Phase 25A
    "planning_enabled": 1,       # Phase 25B
    "planning_interval_sec": 1800,
    "notify": 1,
    "agent_id": "edge-primary,edge-nl1"
  }
}

Safety
- NEVER enqueues commands in Phase 25B.
- If DB/Sheets are unavailable, it no-ops quietly.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List
from utils import str_or_empty, safe_float  # type: ignore


_LOG_ONCE = set()


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


def enabled() -> bool:
    c = _cfg()
    if "enabled" in c:
        return _truthy(c.get("enabled"))
    return False


def planning_enabled() -> bool:
    c = _cfg()
    if "planning_enabled" in c:
        return _truthy(c.get("planning_enabled"))
    return False


def notify() -> bool:
    c = _cfg()
    return _truthy(c.get("notify", 0))


def planning_interval_sec() -> int:
    c = _cfg()
    try:
        v = int(c.get("planning_interval_sec") or 1800)
        return max(120, min(v, 12 * 3600))
    except Exception:
        return 1800


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _derive_simple_plan(decision: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a Phase 25A decision record into a Phase 25B "plan object".

    IMPORTANT:
    - This creates *proposed* intents only.
    - It does not enqueue anything (Phase25C handles that, and is OFF by default).
    """
    decision_id = decision.get("decision_id") or ""
    agent_id = decision.get("agent_id") or ""
    ok = bool(decision.get("ok"))
    rec = (decision.get("recommendation") or "HOLD").upper()
    reasons = decision.get("reasons") or []
    signals = decision.get("signals") or []
    memory = decision.get("memory") or {}

    ts = _now_ts()
    plan_id = os.urandom(8).hex()

    proposed = []
    mode = "planning_only"

    if (not ok) or (rec == "HOLD"):
        proposed = []
        mode = "hold"
    else:
        # Conservative sizing caps (env override)
        max_trade_usd = float(os.getenv("PHASE25_MAX_TRADE_USD", os.getenv("POLICY_CANARY_MAX_USD", "25")))
        prefer_venues = (os.getenv("ROUTER_ALLOWED", "COINBASE,BINANCEUS,KRAKEN").split(",") if os.getenv("ROUTER_ALLOWED") else ["COINBASE","BINANCEUS","KRAKEN"])
        prefer_venues = [v.strip().upper() for v in prefer_venues if v.strip()]

        # Best-effort prefer quote map from policy_engine (if available)
        prefer_quotes = {}
        try:
            from policy_engine import PolicyEngine  # type: ignore
            pe = PolicyEngine()
            prefer_quotes = (pe.cfg.get("prefer_quotes") or {})
        except Exception:
            prefer_quotes = {}

        # Build a quick position USD map from Vaults (read-only)
        pos_usd = {}
        try:
            from phase25_vault_signals import VAULT_TAB, _read_records_prefer_db  # type: ignore
            for r in _read_records_prefer_db(VAULT_TAB) or []:
                tok = str_or_empty(r.get("Token") or r.get("token") or r.get("Asset")).upper()
                if not tok:
                    continue
                v = safe_float(r.get("USD Value") or r.get("usd_value") or r.get("Value_USD") or r.get("Value"))
                if v is None:
                    continue
                pos_usd[tok] = float(v)
        except Exception:
            pass

        def _pick_venue_quote(token: str) -> tuple[str, str]:
            venue = prefer_venues[0] if prefer_venues else "BINANCEUS"
            quote = (prefer_quotes.get(venue) or os.getenv("DEFAULT_QUOTE") or "USDT").upper()
            return venue, quote

        # Translate signals -> proposed TRADE intents (still evaluated by policy/guard)
        # Order: SELL first, then REBUY. Cap count.
        ordered = []
        for s in signals:
            if not isinstance(s, dict):
                continue
            typ = (s.get("type") or "").upper()
            if typ in ("SELL_CANDIDATE","REBUY_CANDIDATE"):
                ordered.append(s)
        # stable sort
        ordered.sort(key=lambda s: (0 if (s.get("type") or "").upper()=="SELL_CANDIDATE" else 1, -float(s.get("confidence") or 0.0)))

        for s in ordered:
            typ = (s.get("type") or "").upper()
            token = (s.get("token") or "").upper()
            if not token:
                continue

            venue, quote = _pick_venue_quote(token)
            action = "SELL" if typ=="SELL_CANDIDATE" else "BUY"
            # sizing: sell up to position usd; rebuy up to cap
            amt = max_trade_usd
            if action == "SELL":
                amt = min(max_trade_usd, float(pos_usd.get(token) or max_trade_usd))

            item = {
                "type": "TRADE",
                "action": action,
                "token": token,
                "venue": venue,
                "quote": quote,
                "amount_usd": float(max(0.0, amt)),
                "mode": "dryrun",
                "reason": "phase25B_plan",
                "decision_id": decision_id,
            }
            proposed.append(item)

            # Keep it small; Phase25C has additional caps too.
            if len(proposed) >= int(os.getenv("PHASE25_PLAN_MAX_ITEMS", "3")):
                break

        # Planning annotations (Phase 25-safe): add non-executable notes when WATCH signals exist.
        # This helps you see "why we are waiting" without enabling any behavior.
        watch = [s for s in signals if isinstance(s, dict) and str(s.get("type") or "").upper() in ("WATCH", "ALPHA_WATCH")]
        if watch:
            # keep it small / low noise
            brief = []
            for s in watch[:5]:
                tok = str(s.get("token") or "").upper()
                rs = s.get("reasons") or []
                r0 = rs[0] if isinstance(rs, list) and rs else ""
                brief.append(f"{tok}: {r0}".strip())

            proposed.append({
                "type": "PLAN_NOTE",
                "action": "NOTE",
                "agent_id": agent_id,
                "reason": "phase25B_watch_notes",
                "notes": "; ".join([b for b in brief if b])[:500],
                "memory": memory if isinstance(memory, dict) else {},
            })

        # Always include a low-risk BALANCE_SNAPSHOT (helps validate budgets)
        proposed.append({
            "type": "BALANCE_SNAPSHOT",
            "action": "BALANCE_SNAPSHOT",
            "agent_id": agent_id,
            "reason": "phase25B_plan"
        })

    # Evaluate each proposed item through guard + policy (for explanations)
    evaluated = []
    for it in proposed:
        if not isinstance(it, dict):
            continue
        typ = str(it.get("type") or "").upper()
        if typ != "TRADE":
            evaluated.append(it)
            continue

        legacy_intent = {
            "token": (it.get("token") or "").upper(),
            "action": (it.get("action") or "BUY").upper(),
            "amount_usd": it.get("amount_usd"),
            "venue": (it.get("venue") or "").upper(),
            "quote": (it.get("quote") or "").upper(),
            "source": "phase25_plan",
            "id": f"p25-{plan_id}",
        }

        guard = None
        pol = None
        try:
            from trade_guard import guard_trade_intent  # type: ignore
            guard = guard_trade_intent(legacy_intent)
        except Exception as e:
            guard = {"ok": False, "status": "error", "reason": f"guard_error:{e.__class__.__name__}:{e}"}

        try:
            from policy_engine import PolicyEngine  # type: ignore
            pe = PolicyEngine()
            ok2, reason2, patched2 = pe.validate(legacy_intent, None)
            pol = {"ok": bool(ok2), "reason": str(reason2), "patched": patched2 or {}}
        except Exception as e:
            pol = {"ok": False, "reason": f"policy_error:{e.__class__.__name__}:{e}", "patched": {}}

        it2 = dict(it)
        it2["guard"] = guard
        it2["policy"] = pol
        evaluated.append(it2)

    return {
        "ok": bool(ok),
        "plan_id": plan_id,
        "ts": ts,
        "phase": "25B",
        "mode": mode,
        "agent_id": agent_id,
        "decision_id": decision_id,
        "recommendation": rec,
        "reasons": reasons,
        "proposed": evaluated,
    }


def _db_write_plan(plan: Dict[str, Any]) -> None:
    # Best-effort write to Postgres. Safe to skip on any error.
    try:
        from db_backbone import _get_conn  # type: ignore
    except Exception:
        return
    try:
        conn = _get_conn()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nova_plans (
              plan_id   TEXT PRIMARY KEY,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              agent_id  TEXT,
              decision_id TEXT,
              phase     TEXT,
              mode      TEXT,
              enqueue   BOOLEAN,
              payload   JSONB
            );
            """
        )
        cur.execute(
            """
            INSERT INTO nova_plans(plan_id, agent_id, decision_id, phase, mode, enqueue, payload)
            VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)
            ON CONFLICT (plan_id) DO NOTHING;
            """,
            (
                plan.get("plan_id"),
                plan.get("agent_id"),
                plan.get("decision_id"),
                plan.get("phase"),
                plan.get("mode"),
                bool(plan.get("enqueue")),
                json.dumps(plan, default=str),
            ),
        )
        conn.commit()
    except Exception:
        return


def _policy_log_plan(plan: Dict[str, Any], decision: Dict[str, Any]) -> None:
    try:
        from policy_logger import log_decision as _log_policy_decision
    except Exception:
        _log_once("Phase25B: policy_logger missing; skipping plan log")
        return

    intent = {
        "token": "",
        "action": "PLAN",
        "amount_usd": 0,
        "venue": "",
        "quote": "",
        "notes": f"plan_id={plan.get('plan_id')} decision_id={plan.get('decision_id')}",
        "source": "phase25_planning_only",
    }
    try:
        payload = {
            "plan": plan,
            "decision": {k: decision.get(k) for k in ("decision_id", "ts", "agent_id", "ok", "recommendation", "reasons")},
        }
        _log_policy_decision(payload, intent, when=plan.get("ts"))
    except Exception:
        _log_once("Phase25B: policy log failed")


def _notify_telegram(plan: Dict[str, Any]) -> None:
    if not notify():
        return
    try:
        from telegram_summaries import send_telegram  # de-duped sender
    except Exception:
        try:
            from nova_trigger import nova_trigger as send_telegram
        except Exception:
            return

    n = len(plan.get("proposed") or [])
    msg = f"🧾 Phase25B Plan: enqueue={plan.get('enqueue')} proposed={n} mode={plan.get('mode')} agent={plan.get('agent_id')} plan_id={plan.get('plan_id')}"
    try:
        send_telegram(msg)
    except Exception:
        pass


def run_phase25_plan_cycle() -> Dict[str, Any]:
    if not enabled():
        _log_once("Phase25B: disabled (set DB_READ_JSON.phase25.enabled=1)")
        return {"ok": False, "skipped": True, "reason": "disabled"}
    if not planning_enabled():
        _log_once("Phase25B: planning not enabled (set DB_READ_JSON.phase25.planning_enabled=1)")
        return {"ok": False, "skipped": True, "reason": "planning_disabled"}

    try:
        from phase25_decision_only import build_decision  # Phase 25A
    except Exception:
        _log_once("Phase25B: missing phase25_decision_only; skipping")
        return {"ok": False, "skipped": True, "reason": "missing_phase25A"}

    decision = build_decision()
    plan = _derive_simple_plan(decision)

    _db_write_plan(plan)
    _policy_log_plan(plan, decision)
    _notify_telegram(plan)

    return {"ok": True, "plan": plan, "decision": decision}


_thread_started = False


def start_phase25b_background_loop() -> None:
    global _thread_started
    if _thread_started:
        return
    if not enabled() or not planning_enabled():
        return
    _thread_started = True

    interval = planning_interval_sec()

    def _loop():
        _log_once(f"🧾 Phase25B planning loop started (interval={interval}s)")
        while True:
            try:
                run_phase25_plan_cycle()
            except Exception:
                pass
            time.sleep(interval)

    threading.Thread(target=_loop, name="phase25-planning", daemon=True).start()


if __name__ == "__main__":
    out = run_phase25_plan_cycle()
    print(_safe_json(out))
