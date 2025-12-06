import os
import json
from typing import Dict, Any, List, Tuple, Optional

from utils import (
    info,
    warn,
    get_ws_cached,
    with_sheet_backoff,
)
from trade_guard import guard_trade_intent

# Optional: Outbox helper (used only in live mode)
try:
    from ops_sign_and_enqueue import attempt as outbox_attempt  # type: ignore
except Exception:
    outbox_attempt = None  # type: ignore

# Only let the stalled-autotrader consider these venues for now.
ALLOWED_VENUES = {"COINBASE", "BINANCEUS"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_enabled(var: str, default: str = "0") -> bool:
    return os.getenv(var, default).strip().lower() in {"1", "true", "yes", "on"}


def _parse_bool(val: Any) -> bool:
    s = str(val).strip().lower()
    return s in {"1", "true", "yes", "y"}


@with_sheet_backoff
def _load_policy_rows() -> List[Dict[str, Any]]:
    """Load Policy_Log rows via the same utils adapter as the rest of Nova."""
    ws = get_ws_cached("Policy_Log")
    rows = ws.get_all_records() or []
    return rows


def _iter_stalled_autoresized_candidates(
    rows: List[Dict[str, Any]]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Yield (row, intent) pairs for auto_resized stalled-asset BUYs
    on allowed venues, with basic amount/price fields populated.
    """
    # Look at the most recent ~200 rows only, from newest → oldest.
    recent = rows[-200:]
    recent.reverse()

    for row in recent:
        source = str(row.get("Source", "")).strip()
        action = str(row.get("Action", "")).strip().upper()
        venue = str(row.get("Venue", "")).strip().upper()
        ok_flag = _parse_bool(row.get("OK", ""))

        if not source.startswith("bus/stalled_asset_detector"):
            continue
        if action != "BUY":
            continue
        if not ok_flag:
            continue
        if venue not in ALLOWED_VENUES:
            continue

        cooldown = str(
            row.get("Cooldown Notes")  # newer header
            or row.get("Notes")        # older / current header
            or ""
        ).lower()
        if "auto_resized" not in cooldown:
            continue

        # Parse Decision JSON (where patched_intent lives)
        decision_raw = row.get("Decision", "") or "{}"
        try:
            decision = json.loads(decision_raw)
        except Exception as e:
            warn(f"[stalled_autotrader] bad Decision JSON: {e} :: {decision_raw[:120]!r}")
            continue

        patched = decision.get("patched_intent") or {}
        if not isinstance(patched, dict) or not patched:
            warn("[stalled_autotrader] no patched_intent in Decision; skipping row")
            continue

        token = (patched.get("token") or row.get("Token") or "").strip().upper()
        quote = (patched.get("quote") or row.get("Quote") or "").strip().upper()
        symbol = patched.get("symbol") or row.get("Symbol") or ""

        # Amount + price
        amount_usd = (
            patched.get("amount_usd")
            or patched.get("amount")
            or row.get("Amount_USD")
        )
        price_usd = patched.get("price_usd") or row.get("Price_USD")

        try:
            amount_usd_f = float(amount_usd)
        except Exception:
            warn(f"[stalled_autotrader] invalid amount_usd={amount_usd!r} for row; skipping")
            continue

        try:
            price_usd_f = float(price_usd)
        except Exception:
            warn(f"[stalled_autotrader] missing/invalid price_usd for {token} @ {venue}; skipping")
            continue

        if not token or not quote or amount_usd_f <= 0:
            warn(
                "[stalled_autotrader] incomplete intent fields for row: "
                f"token={token!r}, quote={quote!r}"
            )
            continue

        intent = {
            "token": token,
            "venue": venue,
            "quote": quote,
            "symbol": symbol,
            "amount_usd": amount_usd_f,
            "price_usd": price_usd_f,
            "action": "BUY",
            "source": "stalled_autotrader",
        }

        yield row, intent


def _pick_agent_id() -> str:
    """
    Resolve which Edge Agent to target.

    Priority:
      1) STALLED_AUTOTRADER_AGENT_ID
      2) DEFAULT_AGENT_TARGET or AGENT_ID (first in comma list)
      3) "edge-primary"
    """
    raw = (
        os.getenv("STALLED_AUTOTRADER_AGENT_ID")
        or os.getenv("DEFAULT_AGENT_TARGET")
        or os.getenv("AGENT_ID")
        or "edge-primary"
    )
    agent_id = raw.split(",")[0].strip()
    return agent_id or "edge-primary"


def _build_outbox_envelope(patched: Dict[str, Any]) -> Dict[str, Any]:
    """
    Shape a patched intent into an Outbox envelope suitable for ops_sign_and_enqueue.

    We:
      - Ensure venue/token/quote/symbol are present.
      - Derive base 'amount' from amount_usd/price_usd if needed.
      - Set type='order.place' and side='BUY'.
      - Tag source='stalled_autotrader'.
    """
    token = (patched.get("token") or "").upper()
    venue = (patched.get("venue") or "").upper()
    quote = (patched.get("quote") or "").upper() or "USDT"

    symbol = patched.get("symbol")
    if not symbol and token and quote:
        symbol = f"{token}/{quote}"

    amount_usd = 0.0
    try:
        amount_usd = float(patched.get("amount_usd") or 0.0)
    except Exception:
        pass

    price_usd = 0.0
    try:
        price_usd = float(patched.get("price_usd") or 0.0)
    except Exception:
        pass

    amount = patched.get("amount")
    if amount is None and amount_usd > 0 and price_usd > 0:
        amount = amount_usd / price_usd

    intent_payload: Dict[str, Any] = dict(patched)
    intent_payload.setdefault("token", token)
    intent_payload.setdefault("venue", venue)
    intent_payload.setdefault("quote", quote)
    intent_payload.setdefault("symbol", symbol)
    intent_payload.setdefault("source", "stalled_autotrader")
    intent_payload.setdefault("side", "BUY")
    intent_payload.setdefault("type", "order.place")
    if amount is not None:
        intent_payload.setdefault("amount", amount)

    agent_id = _pick_agent_id()

    envelope: Dict[str, Any] = {
        "agent_id": agent_id,
        "intent": intent_payload,
        "meta": {
            "source": "stalled_autotrader",
            "origin": "bus/stalled_asset_detector",
        },
    }
    return envelope


# ---------------------------------------------------------------------------
# Shadow mode (existing behaviour, unchanged)
# ---------------------------------------------------------------------------

def run_stalled_autotrader_shadow() -> None:
    """
    Shadow-mode driver: evaluates stalled-asset auto_resized decisions
    through trade_guard, logs what WOULD be sent to Outbox, but does NOT
    actually enqueue any commands.
    """
    if not _is_enabled("STALLED_AUTOTRADER_ENABLED", "0"):
        info("[stalled_autotrader] disabled via STALLED_AUTOTRADER_ENABLED; skipping.")
        return

    max_usd = float(os.getenv("STALLED_AUTOTRADER_MAX_USD", "25"))
    max_trades = int(os.getenv("STALLED_AUTOTRADER_MAX_TRADES_PER_RUN", "2"))

    rows = _load_policy_rows()
    if not rows:
        info("[stalled_autotrader] Policy_Log empty; nothing to do.")
        return

    info(
        "[stalled_autotrader] (shadow) scanning Policy_Log "
        f"(rows={len(rows)}) max_usd={max_usd} max_trades={max_trades}"
    )

    taken = 0
    for row, intent in _iter_stalled_autoresized_candidates(rows):
        if taken >= max_trades:
            break

        # Enforce a hard cap at this layer too.
        if intent["amount_usd"] > max_usd:
            warn(
                "[stalled_autotrader] candidate over max_usd cap "
                f"({intent['amount_usd']} > {max_usd}); skipping."
            )
            continue

        # Final guard pass (venue min-notional, Kraken gate, budgets, policy)
        res = guard_trade_intent(intent)
        ok = bool(res.get("ok"))
        status = res.get("status")
        reason = res.get("reason", "")
        patched = res.get("patched") or {}
        patched_amount = patched.get("amount_usd", intent["amount_usd"])

        info(
            "[stalled_autotrader_shadow] candidate "
            f"venue={intent['venue']} token={intent['token']} quote={intent['quote']} "
            f"amount_usd={intent['amount_usd']} -> guard ok={ok} status={status} "
            f"reason={reason} patched_amount_usd={patched_amount}"
        )

        # SHADOW ONLY: do not enqueue yet
        taken += 1

    if taken == 0:
        info("[stalled_autotrader] no eligible stalled-asset candidates this run.")
    else:
        info(
            f"[stalled_autotrader] evaluated {taken} stalled-asset candidate(s) (shadow mode)."
        )


# ---------------------------------------------------------------------------
# Live, guarded mode (Phase 20)
# ---------------------------------------------------------------------------

def run_stalled_autotrader_live() -> None:
    """
    Live, guarded mode: after passing guard_trade_intent, enqueue
    a properly-shaped Outbox command via ops_sign_and_enqueue.

    Safety:
      - Requires STALLED_AUTOTRADER_ENABLED=1.
      - Respects STALLED_AUTOTRADER_MAX_USD and STALLED_AUTOTRADER_MAX_TRADES_PER_RUN.
      - If ops_sign_and_enqueue is unavailable or misconfigured, we log and
        fall back to a no-op (no trades are sent).
    """
    if not _is_enabled("STALLED_AUTOTRADER_ENABLED", "0"):
        info("[stalled_autotrader_live] disabled via STALLED_AUTOTRADER_ENABLED; skipping.")
        return

    if outbox_attempt is None:
        warn(
            "[stalled_autotrader_live] ops_sign_and_enqueue not importable; "
            "running in EFFECTIVE SHADOW mode (no enqueues)."
        )

    max_usd = float(os.getenv("STALLED_AUTOTRADER_MAX_USD", "25"))
    max_trades = int(os.getenv("STALLED_AUTOTRADER_MAX_TRADES_PER_RUN", "2"))

    rows = _load_policy_rows()
    if not rows:
        info("[stalled_autotrader_live] Policy_Log empty; nothing to do.")
        return

    info(
        "[stalled_autotrader_live] scanning Policy_Log "
        f"(rows={len(rows)}) max_usd={max_usd} max_trades={max_trades}"
    )

    sent = 0
    for row, intent in _iter_stalled_autoresized_candidates(rows):
        if sent >= max_trades:
            break

        if intent["amount_usd"] > max_usd:
            warn(
                "[stalled_autotrader_live] candidate over max_usd cap "
                f"({intent['amount_usd']} > {max_usd}); skipping."
            )
            continue

        res = guard_trade_intent(intent)
        ok = bool(res.get("ok"))
        status = res.get("status")
        reason = res.get("reason", "")
        patched = res.get("patched") or {}
        patched_amount = patched.get("amount_usd", intent["amount_usd"])

        info(
            "[stalled_autotrader_live] candidate "
            f"venue={intent['venue']} token={intent['token']} quote={intent['quote']} "
            f"amount_usd={intent['amount_usd']} -> guard ok={ok} status={status} "
            f"reason={reason} patched_amount_usd={patched_amount}"
        )

        if not ok:
            # Policy or guard said "no" — do NOT enqueue.
            continue

        if outbox_attempt is None:
            # Helper missing; log but keep behaviour non-lethal.
            warn(
                "[stalled_autotrader_live] outbox helper unavailable; "
                "skipping enqueue for eligible candidate."
            )
            sent += 1
            continue

        envelope = _build_outbox_envelope(patched or intent)
        try:
            enq_res = outbox_attempt(envelope)
        except Exception as e:
            warn(f"[stalled_autotrader_live] enqueue exception: {e!r}")
            continue

        enq_ok = bool(enq_res.get("ok"))
        enq_reason = enq_res.get("reason") or ""
        status_code = enq_res.get("status")

        info(
            "[stalled_autotrader_live] enqueue result "
            f"venue={envelope['intent'].get('venue')} "
            f"token={envelope['intent'].get('token')} "
            f"amount_usd={envelope['intent'].get('amount_usd')} "
            f"ok={enq_ok} status={status_code} reason={enq_reason!r}"
        )

        if enq_ok:
            sent += 1

    if sent == 0:
        info("[stalled_autotrader_live] no stalled-asset trades enqueued this run.")
    else:
        info(f"[stalled_autotrader_live] enqueued {sent} stalled-asset trade(s).")


# ---------------------------------------------------------------------------
# Public entrypoint for scheduler
# ---------------------------------------------------------------------------

def run_stalled_autotrader() -> None:
    """
    Orchestrator entrypoint.

    Chooses between SHADOW and LIVE based on STALLED_AUTOTRADER_MODE:

      - (missing) / 'shadow' / 'dryrun'  -> run_stalled_autotrader_shadow()
      - 'live' / 'live_guarded'         -> run_stalled_autotrader_live()
      - any other value                 -> warn and fall back to shadow
    """
    mode = os.getenv("STALLED_AUTOTRADER_MODE", "shadow").strip().lower()

    if mode in {"", "shadow", "dryrun"}:
        info(f"[stalled_autotrader] mode={mode or 'shadow'} -> SHADOW")
        run_stalled_autotrader_shadow()
        return

    if mode in {"live", "live_guarded"}:
        info(f"[stalled_autotrader] mode={mode} -> LIVE (guarded)")
        run_stalled_autotrader_live()
        return

    warn(
        f"[stalled_autotrader] unknown STALLED_AUTOTRADER_MODE={mode!r}; "
        "falling back to SHADOW."
    )
    run_stalled_autotrader_shadow()
