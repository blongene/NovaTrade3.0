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
    # Heuristic plan builder. Conservative by default.
    decision_id = decision.get("decision_id") or ""
    agent_id = decision.get("agent_id") or ""
    ok = bool(decision.get("ok"))
    rec = (decision.get("recommendation") or "HOLD").upper()
    reasons = decision.get("reasons") or []

    # Extract a couple inputs (best-effort)
    inputs = decision.get("inputs") or {}
    wallet = inputs.get("wallet_monitor") or {}
    last_wallet = wallet.get("last") or {}
    free = None
    asset = None
    try:
        free = float(last_wallet.get("Free")) if last_wallet.get("Free") is not None else None
        asset = str(last_wallet.get("Asset") or "")
    except Exception:
        pass

    proposed: List[Dict[str, Any]] = []
    mode = "planning_only"

    if not ok or rec == "HOLD":
        proposed = []
        mode = "hold"
    else:
        # Conservative default: produce a "SCAN" intent only, not a trade.
        proposed.append({
            "type": "SCAN",
            "action": "ROTATION_SCAN",
            "agent_id": agent_id,
            "reason": "phase25B_planning_only"
        })

        # Optional gentle heuristic: if we see a USD/USDT free balance, suggest a small *candidate* buy,
        # but keep amount_usd=0 unless explicitly enabled later in Phase 25C.
        if free is not None and free > 25 and asset in {"USD", "USDT", "USDC"}:
            proposed.append({
                "type": "TRADE_CANDIDATE",
                "action": "BUY",
                "venue": "",
                "symbol": "",
                "amount_usd": 0,
                "reason": f"found_free_{asset}>{free:.2f}_candidate_only"
            })

    plan_id = os.urandom(8).hex()
    return {
        "ok": True,
        "plan_id": plan_id,
        "ts": _now_ts(),
        "phase": "25B",
        "mode": mode,
        "enqueue": False,
        "decision_id": decision_id,
        "agent_id": agent_id,
        "summary": rec,
        "reasons": reasons,
        "proposed": proposed,
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
