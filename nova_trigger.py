# nova_trigger.py — Manual command router with B-2 Price Feed (Bus side).
# - Routes MANUAL_REBUY commands to the policy engine for validation.
# - Auto-injects price_usd from Unified_Snapshot to resolve "price unknown" denials.
# - Uses SAFE IMPORTS to prevent crashes if utils.py is mid-update.

import os, json, time, re, hmac, hashlib, uuid
from typing import Any, Dict, Optional, Tuple, List

import requests  # NEW: for fallback enqueue

# 1. Safe Import Block
try:
    from utils import (
        SHEET_URL,
        get_ws,
        get_ws_cached,
        get_sheet,
        warn,
        info,
        send_telegram_message_dedup,
        hmac_enqueue,  # prefer canonical utils version if present
    )
except ImportError:
    # If utils doesn't export hmac_enqueue, fall back to a local HMAC client.
    from utils import get_ws, get_sheet, warn, info, send_telegram_message_dedup

    def _canon(body: Dict[str, Any]) -> bytes:
        return json.dumps(body, separators=(",", ":"), sort_keys=True).encode()

    def hmac_enqueue(intent: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[no-redef]
        """
        Fallback HMAC enqueue implementation that talks directly to /api/ops/enqueue.

        Uses:
          OPS_ENQUEUE_URL  (full URL) OR
          OPS_BASE_URL/BASE_URL + /api/ops/enqueue

        Signs with OUTBOX_SECRET/EDGE_SECRET, same scheme as Edge Agent.
        """
        # Resolve URL
        url = os.getenv("OPS_ENQUEUE_URL")
        if not url:
            base = (
                os.getenv("OPS_BASE_URL")
                or os.getenv("BUS_BASE_URL")
                or os.getenv("BASE_URL")
                or ""
            ).rstrip("/")
            if not base:
                warn("nova_trigger: no OPS_ENQUEUE_URL/BASE_URL; cannot enqueue")
                return {"ok": False, "reason": "enqueue_url_missing"}
            url = f"{base}/api/ops/enqueue"

        # Resolve secret and agent
        secret = (
            os.getenv("OUTBOX_SECRET")
            or os.getenv("EDGE_SECRET")
            or os.getenv("BUS_SECRET")
            or ""
        ).strip()
        agent_id = (
            os.getenv("DEFAULT_AGENT_TARGET")
            or os.getenv("AGENT_ID")
            or "edge-primary"
        )

        if not secret:
            warn("nova_trigger: no OUTBOX_SECRET/EDGE_SECRET; cannot sign enqueue")
            return {"ok": False, "reason": "enqueue_secret_missing"}

        # Prepare payload
        payload = {
            "agent_id": agent_id,
            "intent": intent,
        }
        raw = _canon(payload)
        ts = str(int(time.time()))
        mac = hmac.new(secret.encode(), raw + ts.encode(), hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-Signature": f"sha256={mac}",
            "X-Timestamp": ts,
        }

        try:
            r = requests.post(url, data=raw, headers=headers, timeout=15.0)
            try:
                body = r.json()
            except Exception:
                body = {"raw": r.text}
            if not body.get("ok"):
                warn(f"nova_trigger: enqueue failed: {body}")
            return {"ok": bool(body.get("ok")), "reason": body.get("error") or ""}
        except Exception as e:
            warn(f"nova_trigger: enqueue exception: {e}")
            return {"ok": False, "reason": str(e)}


# 2. Manual Rebuy Policy Import (with fallback)
try:
    from manual_rebuy_policy import evaluate_manual_rebuy
except ImportError:
    # Fallback stub if file missing
    def evaluate_manual_rebuy(intent, asset_state):
        return {
            "ok": True,
            "reason": "policy_missing_allow_all",
            "patched_intent": intent,
        }


# B-2 price feed (direct to venues)
try:
    from price_feed import get_price_usd as _feed_get_price_usd
except ImportError:
    def _feed_get_price_usd(
        token: str, quote: str = "USDT", venue: str | None = None
    ):
        warn("nova_trigger: price_feed missing; _feed_get_price_usd returns None")
        return None

# Autonomy status (Phase 20B)
try:
    from autonomy_modes import get_autonomy_state, format_autonomy_status
except Exception:  # fail-open if module not available
    def get_autonomy_state():
        return {}
    def format_autonomy_status(state=None) -> str:
        return ""

# Decision Stories (Phase 20C)
try:
    from decision_story import generate_decision_story
except Exception:  # fail-open if module missing
    def generate_decision_story(intent, decision, autonomy_state=None) -> str:
        # Fallback: just echo the reason
        return str(decision.get("reason") or "")

# === Config & Const

UNIFIED_SNAPSHOT_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")
PRICE_CACHE_TTL_SEC = int(os.getenv("PRICE_CACHE_TTL_SEC", "180"))
TELEGRAM_DEDUP_TTL = int(os.getenv("TELEGRAM_DEDUP_TTL_SEC", "120"))

# Global Cache (In-process memory)
_price_cache = {"ts": 0.0, "rows": []}


# ---------------------------------------------------------------------------
# B-2: Unified_Snapshot price lookup for manual rebuys
# ---------------------------------------------------------------------------
def _load_price_snapshot(force: bool = False) -> List[Dict[str, Any]]:
    global _price_cache
    now = time.time()
    if (
        not force
        and _price_cache.get("ts", 0.0) > 0
        and now - _price_cache["ts"] < PRICE_CACHE_TTL_SEC
    ):
        return _price_cache["rows"]

    try:
        ws = get_ws_cached(SHEET_URL, UNIFIED_SNAPSHOT_WS)
        rows = ws.get_all_records()
        _price_cache["ts"] = now
        _price_cache["rows"] = rows or []
        return _price_cache["rows"]
    except Exception as e:
        warn(f"nova_trigger: Failed to load {UNIFIED_SNAPSHOT_WS} for price feed: {e}")
        return []


def _get_price_usd_from_snapshot(token: str) -> Tuple[Optional[float], str]:
    token_up = (token or "").upper()
    if not token_up:
        return None, "no_token"

    rows = _load_price_snapshot()
    for r in rows:
        row_token = (r.get("Token") or "").upper()
        if row_token != token_up:
            continue
        price = r.get("Price_USD")
        if price is None:
            return None, "no_price"
        try:
            return float(price), "ok"
        except Exception:
            return None, "bad_price"

    return None, "not_found"


def _get_price_usd(token: str, quote: str = "USDT", venue: str | None = None) -> Tuple[Optional[float], str]:
    """
    Price resolution strategy for manual rebuys:

      1) Unified_Snapshot (fast, sheet driven)
      2) Venue-specific feed (price_feed.get_price_usd)
    """
    # 1) Snapshot first
    p, reason = _get_price_usd_from_snapshot(token)
    if p is not None:
        return p, "snapshot"

    # 2) Fallback to venue feed
    p2 = _feed_get_price_usd(token, quote=quote, venue=venue)
    if p2 is None:
        return None, f"no_price ({reason}, feed_none)"
    return p2, "feed"


# ---------------------------------------------------------------------------
# Telegram summary helper
# ---------------------------------------------------------------------------
def _send_summary(
    raw: str,
    intent: dict,
    decision: dict,
    enq_ok: bool,
    enq_reason: Optional[str],
    mode: str,
    story: Optional[str] = None,
) -> None:
    """
    Telegram summary for a manual rebuy.

    Phase 20C: if a decision story is provided, it becomes the primary explanation.
    """
    icon = "✅" if decision.get("ok") else "❌"

    lines = [
        f"{icon} <b>Manual Rebuy</b>",
        f"Cmd: <code>{raw}</code>",
    ]

    # Story (Phase 20C) – preferred explanation
    if story:
        lines.append(f"Story: {story}")
    else:
        # Fallback: legacy policy reason
        lines.append(f"Policy: {decision.get('reason')}")

    # Price line if available
    price_usd = intent.get("price_usd")
    if price_usd is not None:
        try:
            lines.append(f"Price: ${float(price_usd):,.2f}")
        except Exception:
            lines.append(f"Price: {price_usd}")

    # Enqueue / mode line
    if mode == "live":
        e_icon = "🚀" if enq_ok else "⚠️"
        lines.append(f"Enqueue: {e_icon} {enq_reason or 'OK'}")
    else:
        lines.append("Mode: DRYRUN (Not Enqueued)")

    text = "\n".join(lines)

    # utils.send_telegram_message_dedup(message, key, ttl_min=15)
    try:
        send_telegram_message_dedup(
            text,
            key=f"manual_rebuy:{intent.get('token', 'UNKNOWN')}:{intent.get('venue', 'UNKNOWN')}",
            ttl_min=1,
        )
    except Exception as e:
        try:
            warn(f"nova_trigger: Telegram summary failed: {e}")
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Main handler: process MANUAL_REBUY string
# ---------------------------------------------------------------------------
def handle_manual_rebuy(raw: str) -> dict:
    """
    Entry point for a MANUAL_REBUY command, e.g.:

        MANUAL_REBUY BTC 500 VENUE=BINANCEUS

    Returns a dict summarizing policy + enqueue result, plus
    autonomy, notes, story, and council influence.
    """
    parsed = parse_manual(raw)
    if not parsed.get("ok"):
        reason = parsed.get("reason") or "parse_failed"
        # Parse failed before a policy decision existed; treat as
        # a rejected-before-policy event.
        council = {
            "soul": 1.0,
            "nova": 1.0,
            "orion": 0.0,
            "ash": 0.0,
            "lumen": 0.0,
            "vigil": 0.0,
        }
        return {
            "ok": False,
            "reason": reason,
            "decision": None,
            "enqueue": {"ok": False, "reason": "parse_failed"},
            "mode": os.getenv("REBUY_MODE", "dryrun").lower(),
            "autonomy": "rejected_before_policy",
            "notes": reason,
            "story": reason,
            "council": council,
        }

    intent = parsed["intent"]

    # B-2: Auto Price Fetch (snapshot → venue feed)
    price_usd, p_reason = _get_price_usd(
        intent["token"], intent.get("quote") or "USDT", intent.get("venue")
    )
    if price_usd is not None:
        intent["price_usd"] = price_usd
    else:
        try:
            warn(f"nova_trigger: Could not find price for {intent['token']} ({p_reason})")
        except Exception:
            pass

    # Policy Check
    decision = evaluate_manual_rebuy(intent, asset_state={})

    # Ensure every manual rebuy decision carries a stable decision_id
    try:
        if not isinstance(decision, dict):
            decision = {"ok": False, "reason": "invalid_decision_type"}
        if not decision.get("decision_id"):
            decision["decision_id"] = uuid.uuid4().hex
    except Exception:
        pass

    policy_ok = bool(decision.get("ok"))

    enq_ok: bool = False
    enq_reason: Optional[str] = None
    mode = os.getenv("REBUY_MODE", "dryrun").lower()

    if policy_ok and mode == "live":
        try:
            patched = decision.get("patched_intent") or {}
            payload = {
                "venue": patched.get("venue") or intent["venue"],
                "symbol": patched.get("symbol") or f"{intent['token']}/{intent['quote']}",
                "side": patched.get("side") or "BUY",
                "amount": patched.get("amount") or 0.0,
                "amount_quote": patched.get("amount_usd"),
                "source": "manual_rebuy",
                "ts": int(time.time()),
                "decision_id": decision.get("decision_id"),
            }
            if intent.get("price_usd") is not None:
                payload["price_usd"] = intent["price_usd"]

            res = hmac_enqueue(payload)
            enq_ok = bool(res.get("ok"))
            enq_reason = res.get("reason")
        except Exception as e:
            try:
                warn(f"nova_trigger: Enqueue failed: {e}")
            except Exception:
                pass
            enq_reason = str(e)

    # Autonomy classification
    if not policy_ok:
        autonomy = "blocked_by_policy"
        base_notes = decision.get("reason") or "Blocked by policy_engine."
    elif mode != "live":
        autonomy = "dryrun"
        base_notes = "Policy approved but REBUY_MODE is not 'live'; command not enqueued."
    elif policy_ok and not enq_ok:
        autonomy = "live_enqueue_failed"
        base_notes = f"Policy approved but enqueue failed: {enq_reason or 'unknown reason'}."
    else:
        autonomy = "live_enqueued"
        base_notes = "Policy approved and command enqueued successfully."

    # Council influence tagging (20D)
    council = {
        "soul": 1.0,
        "nova": 1.0,
        "orion": 0.0,
        "ash": 0.0,
        "lumen": 0.0,
        "vigil": 0.0,
    }
    try:
        from council_influence import apply_council_influence
        council = apply_council_influence(intent, decision, {"autonomy": autonomy, "mode": mode})
    except Exception as e:
        try:
            warn(f"nova_trigger: council influence tagging failed: {e}")
        except Exception:
            pass

    # Attach back onto the decision for Policy_Log / downstreams
    try:
        decision["autonomy"] = autonomy
        decision["council"] = council
    except Exception:
        pass

    # Story + notes
    story = ""
    try:
        auto_state = {"autonomy": autonomy, "mode": mode}
        story = generate_decision_story(intent, decision, autonomy_state=auto_state)
        decision["story"] = story
    except Exception as e:
        try:
            warn(f"nova_trigger: generate_decision_story failed: {e}")
        except Exception:
            pass
        story = str(decision.get("reason") or "")

    notes = base_notes
    if story and story not in notes:
        notes = f"{story} | {base_notes}"

    # Log to Policy_Log (best-effort)
    try:
        from policy_logger import log_decision as _log_policy_decision
        _log_policy_decision(decision, intent)
    except Exception as _e:
        try:
            warn(f"nova_trigger: policy logging failed: {_e}")
        except Exception:
            pass

    from policy_logger import log_decision_insight
    log_decision_insight(decision, intent)

    # Telegram summary (best-effort; includes story)
    try:
        _send_summary(raw, intent, decision, enq_ok, enq_reason, mode, story)
    except Exception as _e:
        try:
            warn(f"nova_trigger: _send_summary failed: {_e}")
        except Exception:
            pass

    return {
        "ok": policy_ok,
        "decision": decision,
        "enqueue": {"ok": enq_ok, "reason": enq_reason},
        "mode": mode,
        "autonomy": autonomy,
        "notes": notes,
        "story": story,
        "council": council,
    }

def route_manual(raw: str) -> dict:
    """
    Thin wrapper so nova_trigger_watcher and other callers can import route_manual.
    Delegates to handle_manual_rebuy(raw).
    """
    return handle_manual_rebuy(raw)

def parse_manual(raw: str) -> dict:
    # MANUAL_REBUY BTC 500 VENUE=BINANCEUS
    parts = raw.strip().split()
    if len(parts) < 3:
        return {"ok": False, "reason": "formatting_error"}

    cmd = parts[0].upper()
    if cmd != "MANUAL_REBUY":
        return {"ok": False, "reason": "not_manual_rebuy"}

    token = parts[1].upper()
    try:
        amount_usd = float(parts[2])
    except Exception:
        return {"ok": False, "reason": "amount_not_number"}

    venue = "BINANCEUS"
    quote = "USDT"

    # Parse optional k=v pairs
    for p in parts[3:]:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.upper()
        v = v.upper()
        if k == "VENUE":
            venue = v
        elif k == "QUOTE":
            quote = v

    intent = {
        "token": token,
        "amount_usd": amount_usd,
        "venue": venue,
        "quote": quote,
        "action": "BUY",
        "source": "manual_rebuy",
        "raw_msg": raw,
    }
    return {"ok": True, "intent": intent}


# Optional CLI for quick testing
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python nova_trigger.py 'MANUAL_REBUY BTC 500 VENUE=BINANCEUS'")
        sys.exit(1)

    raw = sys.argv[1]
    out = handle_manual_rebuy(raw)
    print(json.dumps(out, indent=2, default=str))
