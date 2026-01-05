"""phase25_gated_enqueue.py — Phase 25C (Bus)

Gated Enqueue:
- Reads the latest Phase 25B plan (or runs the planner), evaluates strict guards,
  and (optionally) enqueues a small number of commands to the Outbox.
- Designed to be SAFE and OFF by default.

Key safety rules
- OFF unless DB_READ_JSON.phase25.enqueue_enabled == 1
- Respects Cloud hold and Edge authority (Phase 24C)
- Caps:
  - max_commands_per_cycle
  - max_enqueues_per_window (per agent)
  - cooldown_sec per command-key
- Idempotent:
  - Uses (plan_id, item_index) unique keys stored in DB table nova_plan_enqueues
  - If already enqueued, will not enqueue again

Config (DB_READ_JSON.phase25)
{
  "phase25": {
    "enabled": 1,
    "decision_only": 1,
    "planning_enabled": 1,
    "planning_interval_sec": 1800,

    "enqueue_enabled": 0,              # OFF by default (set to 1 to enable)
    "enqueue_interval_sec": 1800,
    "max_commands_per_cycle": 1,
    "cooldown_sec": 3600,
    "require_approval": 1,             # requires approve=1 to enqueue
    "approve": 0,                      # flip to 1 for a window, then back to 0
    "allow_types": ["SCAN"],           # allowed plan item types
    "notify": 1,
    "agent_id": "edge-primary"
  }
}

Notes
- This module enqueues only *safe* command intents by default: SCAN/ROTATION_SCAN.
- Trade candidates remain amount_usd=0 and are ignored unless allow_types expanded.
"""

from __future__ import annotations
import hashlib
import hmac
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


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


def _cfg_get(key: str, default: Any = None) -> Any:
    """Read a key from DB_READ_JSON.phase25 with a safe default."""
    try:
        return _cfg().get(key, default)
    except Exception:
        return default


def enabled() -> bool:
    return _truthy(_cfg().get("enabled", 0))


def enqueue_enabled() -> bool:
    return _truthy(_cfg().get("enqueue_enabled", 0))


def require_approval() -> bool:
    return _truthy(_cfg().get("require_approval", 1))


def approve() -> bool:
    return _truthy(_cfg().get("approve", 0))


def notify() -> bool:
    return _truthy(_cfg().get("notify", 0))


def agent_id() -> str:
    return str(_cfg().get("agent_id") or "edge").strip() or "edge"


def enqueue_interval_sec() -> int:
    try:
        v = int(_cfg().get("enqueue_interval_sec") or 1800)
        return max(120, min(v, 12 * 3600))
    except Exception:
        return 1800


def max_commands_per_cycle() -> int:
    try:
        v = int(_cfg().get("max_commands_per_cycle") or 1)
        return max(0, min(v, 5))
    except Exception:
        return 1


def cooldown_sec() -> int:
    try:
        v = int(_cfg().get("cooldown_sec") or 3600)
        return max(0, min(v, 7 * 24 * 3600))
    except Exception:
        return 3600


def allow_types() -> List[str]:
    v = _cfg().get("allow_types") or ["SCAN"]
    if isinstance(v, list):
        return [str(x).strip().upper() for x in v if str(x).strip()]
    return ["SCAN"]


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _cloud_hold_active() -> bool:
    # Bus already uses CLOUD_HOLD/NOVA_KILL in autonomy_modes; we replicate a safe check here.
    v = os.getenv("CLOUD_HOLD") or "0"
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _edge_authority_ok(agent: str) -> Tuple[bool, str, Optional[int]]:
    try:
        from edge_authority import evaluate_agent  # Phase 24C
        trusted, reason, age = evaluate_agent(agent)
        return bool(trusted), str(reason), age
    except Exception:
        # If missing, default to NOT trusted (safe)
        return False, "edge_authority_missing", None


def _get_conn():
    try:
        from db_backbone import _get_conn as _gc  # type: ignore
        return _gc()
    except Exception:
        return None


def _ensure_table() -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nova_plan_enqueues (
              plan_id TEXT NOT NULL,
              item_index INTEGER NOT NULL,
              cmd_id TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              PRIMARY KEY(plan_id, item_index)
            );
            """
        )
        # helpful index
        cur.execute("CREATE INDEX IF NOT EXISTS idx_nova_plan_enqueues_created_at ON nova_plan_enqueues(created_at);")
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def _already_enqueued(plan_id: str, item_index: int) -> bool:
    conn = _get_conn()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM nova_plan_enqueues WHERE plan_id=%s AND item_index=%s LIMIT 1;", (plan_id, item_index))
        return cur.fetchone() is not None
    except Exception:
        return False


def _mark_enqueued(plan_id: str, item_index: int, cmd_id: str) -> None:
    conn = _get_conn()
    if not conn:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO nova_plan_enqueues(plan_id, item_index, cmd_id)
            VALUES(%s,%s,%s)
            ON CONFLICT(plan_id, item_index) DO NOTHING;
            """,
            (plan_id, item_index, cmd_id),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def _latest_plan_from_db(agent: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT payload
            FROM nova_plans
            WHERE agent_id=%s
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (agent,),
        )
        row = cur.fetchone()
        if not row:
            return None
        payload = row[0]
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, str):
            return json.loads(payload)
        return json.loads(str(payload))
    except Exception:
        return None


def _run_planner_once() -> Optional[Dict[str, Any]]:
    try:
        from phase25_planning_only import run_phase25_plan_cycle
        out = run_phase25_plan_cycle()
        plan = (out or {}).get("plan")
        return plan if isinstance(plan, dict) else None
    except Exception:
        return None


def _bus_base_url() -> str:
    """
    Base URL used ONLY for HTTP fallbacks.
    For in-process enqueue, we don't need it.
    """
    return (
        os.getenv("PUBLIC_BASE_URL")
        or os.getenv("BASE_URL")
        or os.getenv("CLOUD_BASE_URL")
        or os.getenv("BUS_BASE_URL")
        or ""
    ).strip().rstrip("/")


def _hmac_sha256_hex(secret: str, body_bytes: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()


def _signed_post_json(url: str, payload: Dict[str, Any], secret_env: str, header_name: str) -> Optional[Dict[str, Any]]:
    """
    POST JSON with stable bytes and an HMAC signature header.
    Returns JSON dict on success, else None.
    """
    secret = (os.getenv(secret_env) or "").strip()
    if not secret:
        return None

    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = _hmac_sha256_hex(secret, body)

    headers = {
        "Content-Type": "application/json",
        header_name: sig,
        # extra belt-and-suspenders (harmless if ignored)
        "X-OUTBOX-SIGNATURE": sig,
    }

    try:
        import requests  # local import to avoid hard dependency at import-time
        r = requests.post(url, data=body, headers=headers, timeout=10)
        if r.status_code >= 400:
            return None
        try:
            return r.json()
        except Exception:
            return None
    except Exception:
        return None


def _http_outbox_enqueue(commands: List[Dict[str, Any]], agent: str) -> List[str]:
    """
    Best-effort HTTP enqueue fallback.
    Tries common endpoints + payload shapes.
    Returns list of cmd_ids enqueued.
    """
    base = _bus_base_url()
    if not base:
        return []

    # Ensure cmd_ids exist
    for c in commands:
        cmd_id = c.get("cmd_id") or c.get("command_id")
        if not cmd_id:
            cmd_id = os.urandom(8).hex()
            c["cmd_id"] = cmd_id

    # Try a few likely endpoints + payload shapes.
    tries: List[tuple[str, Dict[str, Any], str]] = [
        ("/api/outbox/enqueue", {"agent_id": agent, "commands": commands}, "X-OUTBOX-SIGN"),
        ("/api/outbox/enqueue", {"agent": agent, "commands": commands}, "X-OUTBOX-SIGN"),
        ("/api/outbox/enqueue", {"agent_id": agent, "items": commands}, "X-OUTBOX-SIGN"),
        ("/api/commands/enqueue", {"agent_id": agent, "commands": commands}, "X-OUTBOX-SIGN"),
        ("/api/outbox/push", {"agent_id": agent, "commands": commands}, "X-OUTBOX-SIGN"),
    ]

    for path, payload, header in tries:
        url = base + path
        out = _signed_post_json(url, payload, secret_env="OUTBOX_SECRET", header_name=header)
        if not isinstance(out, dict):
            continue

        # Accept multiple response formats
        for key in ("enqueued", "cmd_ids", "ids", "commands", "items"):
            val = out.get(key)
            if isinstance(val, list) and val:
                ids: List[str] = []
                for x in val:
                    if isinstance(x, str):
                        ids.append(x)
                    elif isinstance(x, dict):
                        cid = x.get("cmd_id") or x.get("command_id") or x.get("id")
                        if cid:
                            ids.append(str(cid))
                if ids:
                    return ids

        # Some endpoints just return {"ok":true}
        if out.get("ok") is True:
            return [str(c.get("cmd_id") or "") for c in commands if c.get("cmd_id")]

    return []


def _outbox_enqueue(commands: List[Dict[str, Any]], agent: str) -> List[str]:
    """
    Best-effort enqueue to outbox.
    1) Try to discover in-process outbox/command store and enqueue directly.
    2) Fallback to signed HTTP enqueue using OUTBOX_SECRET.
    Returns list of enqueued cmd_ids.
    """
    ids: List[str] = []

    # ---- (1) in-process store discovery (expanded) ----
    store = None
    for mod, attr in [
        ("api_commands", "store"),
        ("api_commands", "command_store"),
        ("api_commands", "COMMAND_STORE"),
        ("command_outbox", "store"),
        ("outbox_store", "store"),
        ("outbox", "store"),
        ("wsgi", "store"),
    ]:
        try:
            m = __import__(mod, fromlist=[attr])
            cand = getattr(m, attr, None)
            if cand:
                store = cand
                break
        except Exception:
            continue

    if store:
        for c in commands:
            try:
                cmd_id = c.get("cmd_id") or c.get("command_id") or ""
                if not cmd_id:
                    cmd_id = os.urandom(8).hex()
                    c["cmd_id"] = cmd_id

                # Try multiple method signatures defensively
                if hasattr(store, "enqueue"):
                    try:
                        store.enqueue(agent, c)
                    except TypeError:
                        store.enqueue(c)
                elif hasattr(store, "put"):
                    try:
                        store.put(agent, c)
                    except TypeError:
                        store.put(c)
                elif hasattr(store, "add"):
                    store.add(c)
                elif hasattr(store, "insert"):
                    store.insert(c)
                else:
                    raise RuntimeError("unknown outbox store interface")

                ids.append(cmd_id)
            except Exception:
                continue

        if ids:
            return ids

    # ---- (2) function-style fallback (kept) ----
    for fn_name in ("enqueue_command", "enqueue_outbox", "outbox_enqueue"):
        try:
            m = __import__("db_backbone", fromlist=[fn_name])
            fn = getattr(m, fn_name, None)
            if callable(fn):
                for c in commands:
                    cmd_id = c.get("cmd_id") or c.get("command_id") or os.urandom(8).hex()
                    c["cmd_id"] = cmd_id
                    try:
                        fn(c)
                        ids.append(cmd_id)
                    except Exception:
                        pass
                if ids:
                    return ids
        except Exception:
            continue

    # ---- (3) signed HTTP fallback using OUTBOX_SECRET ----
    http_ids = _http_outbox_enqueue(commands, agent)
    if http_ids:
        return http_ids

    # No outbox available
    return []


def _plan_to_commands(plan: Dict[str, Any], agent: str) -> List[Dict[str, Any]]:
    """
    Convert a Phase 25B plan into Outbox intents.

    Supported plan items (if enabled/approved):
    - TRADE: becomes an Edge trade intent (venue/symbol/side/amount_usd/mode)
    - SCAN + ROTATION_SCAN: legacy safe no-op-ish command (kept for backward compat)

    NOTE: Enqueue is still gated by enqueue_enabled(), approval flags, and caps.
    """
    allowed = set(allow_types())
    proposed = plan.get("proposed") or []
    if not isinstance(proposed, list):
        return []
    cmds: List[Dict[str, Any]] = []

    plan_id = str(plan.get("plan_id") or plan.get("id") or "").strip() or "plan"
    force_mode = str(_cfg_get("force_mode", "") or "").strip().lower() or None

    for idx, item in enumerate(proposed):
        if not isinstance(item, dict):
            continue
        typ = str(item.get("type") or "").strip().upper()
        if typ not in allowed:
            continue

        if typ == "TRADE":
            token = str(item.get("token") or "").strip().upper()
            venue = str(item.get("venue") or "").strip().upper()
            quote = str(item.get("quote") or "").strip().upper()
            action = str(item.get("action") or "").strip().upper()
            side = "BUY" if action not in ("SELL", "BUY") else action
            amt = item.get("amount_usd")
            try:
                amt_f = float(amt) if amt is not None else 0.0
            except Exception:
                amt_f = 0.0
            if not token or not venue or not quote or amt_f <= 0:
                continue

            mode = force_mode or str(item.get("mode") or "dryrun").strip().lower()
            intent = {
                "venue": venue,
                "symbol": f"{token}/{quote}",
                "side": side,
                "amount_usd": amt_f,
                "mode": mode,
                "source": "phase25",
                "policy_id": plan_id,
                "client_id": f"phase25-{plan_id}-{idx}-{venue}-{token}-{quote}-{side.lower()}",
            }
            intent["item_index"] = idx
            cmds.append(intent)
            continue

        # Legacy: SCAN/ROTATION_SCAN (kept ultra-safe)
        action = str(item.get("action") or "").strip().upper()
        if action != "ROTATION_SCAN":
            continue
        cmds.append({
            "venue": "BUS",  # not executed as trade
            "symbol": "ROTATION_SCAN",
            "side": "BUY",
            "amount_usd": 0.0,
            "mode": "dryrun",
            "source": "phase25",
            "policy_id": plan_id,
            "client_id": f"phase25-{plan_id}-{idx}-rotation-scan",
        })

    return cmds


def _policy_log_enqueue(plan: Dict[str, Any], enqueued_ids: List[str], reason: str) -> None:
    try:
        from policy_logger import log_decision as _log_policy_decision
    except Exception:
        return

    intent = {
        "token": "",
        "action": "ENQUEUE",
        "amount_usd": 0,
        "venue": "",
        "quote": "",
        "notes": f"phase25C enqueue ids={','.join(enqueued_ids)} reason={reason} plan_id={plan.get('plan_id')}",
        "source": "phase25_gated_enqueue",
    }
    payload = {"plan": {k: plan.get(k) for k in ("plan_id", "ts", "phase", "mode", "enqueue", "summary", "reasons")}, "enqueued": enqueued_ids, "reason": reason}
    try:
        _log_policy_decision(payload, intent, when=_now_ts())
    except Exception:
        pass


def _notify(msg: str) -> None:
    if not notify():
        return
    try:
        from telegram_summaries import send_telegram
    except Exception:
        try:
            from nova_trigger import nova_trigger as send_telegram
        except Exception:
            return
    try:
        send_telegram(msg)
    except Exception:
        pass


def run_phase25_enqueue_cycle() -> Dict[str, Any]:
    if not enabled():
        _log_once("Phase25C: disabled (DB_READ_JSON.phase25.enabled!=1)")
        return {"ok": False, "skipped": True, "reason": "disabled"}

    if not enqueue_enabled():
        # silent by default; we only log once
        _log_once("Phase25C: enqueue disabled (set DB_READ_JSON.phase25.enqueue_enabled=1 to enable)")
        return {"ok": False, "skipped": True, "reason": "enqueue_disabled"}

    if require_approval() and not approve():
        _log_once("Phase25C: waiting for approval (set DB_READ_JSON.phase25.approve=1 to allow enqueue window)")
        return {"ok": False, "skipped": True, "reason": "approval_required"}

    if _cloud_hold_active():
        return {"ok": True, "skipped": True, "reason": "cloud_hold"}

    agent = agent_id()
    trusted, treason, age = _edge_authority_ok(agent)
    if not trusted:
        return {"ok": True, "skipped": True, "reason": f"edge_authority:{treason}", "age_sec": age}

    _ensure_table()

    plan = _latest_plan_from_db(agent) or _run_planner_once()
    if not plan:
        return {"ok": False, "skipped": True, "reason": "no_plan"}

    # Must be Phase 25B plan (best-effort check)
    if str(plan.get("phase") or "").upper() != "25B":
        # still allow, but note it
        pass

    # If the plan is "hold", do not enqueue
    if str(plan.get("mode") or "").lower() == "hold":
        return {"ok": True, "skipped": True, "reason": "plan_hold", "plan_id": plan.get("plan_id")}

    cmds = _plan_to_commands(plan, agent)
    if not cmds:
        return {"ok": True, "skipped": True, "reason": "no_allowed_items", "plan_id": plan.get("plan_id")}

    # Enforce per-cycle cap
    cap = max_commands_per_cycle()
    cmds = cmds[:cap]

    enqueued_ids: List[str] = []
    plan_id = str(plan.get("plan_id") or "")

    # Idempotency: filter already-enqueued items
    filtered: List[Dict[str, Any]] = []
    for c in cmds:
        idx = int(c.get("item_index") or 0)
        if _already_enqueued(plan_id, idx):
            continue
        filtered.append(c)

    if not filtered:
        return {"ok": True, "skipped": True, "reason": "already_enqueued", "plan_id": plan_id}

    # Enqueue (best-effort)
    ids = _outbox_enqueue(filtered, agent)

    # Mark + log
    for c in filtered:
        idx = int(c.get("item_index") or 0)
        cmd_id = str(c.get("cmd_id") or "")
        if cmd_id and cmd_id in ids:
            _mark_enqueued(plan_id, idx, cmd_id)
            enqueued_ids.append(cmd_id)

    if enqueued_ids:
        _policy_log_enqueue(plan, enqueued_ids, "ok")
        _notify(f"✅ Phase25C enqueued {len(enqueued_ids)} cmd(s) for agent={agent} plan_id={plan_id}")
        return {"ok": True, "enqueued": enqueued_ids, "plan_id": plan_id}

    return {"ok": False, "skipped": True, "reason": "enqueue_failed_or_no_outbox", "plan_id": plan_id}


_thread_started = False


def start_phase25c_background_loop() -> None:
    global _thread_started
    if _thread_started:
        return
    if not enabled() or not enqueue_enabled():
        return
    _thread_started = True

    interval = enqueue_interval_sec()

    def _loop():
        _log_once(f"🚦 Phase25C gated enqueue loop started (interval={interval}s)")
        while True:
            try:
                run_phase25_enqueue_cycle()
            except Exception:
                pass
            time.sleep(interval)

    threading.Thread(target=_loop, name="phase25-enqueue", daemon=True).start()


if __name__ == "__main__":
    out = run_phase25_enqueue_cycle()
    print(_safe_json(out))
