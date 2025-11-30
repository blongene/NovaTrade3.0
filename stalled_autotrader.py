import os
import json
from typing import Dict, Any, List, Tuple

from utils import info, warn
from sheets import get_ws, get_records
from trade_guard import guard_trade_intent

# Only let the stalled-autotrader consider these venues for now.
ALLOWED_VENUES = {"COINBASE", "BINANCEUS"}

# Helper for env flags
def _is_enabled(var: str, default: str = "0") -> bool:
    return os.getenv(var, default).strip().lower() in {"1", "true", "yes", "on"}


def _parse_bool(val: Any) -> bool:
    s = str(val).strip().lower()
    return s in {"1", "true", "yes", "y"}


def _load_policy_rows() -> List[Dict[str, Any]]:
    """Load Policy_Log rows via Sheets adapter."""
    ws = get_ws("Policy_Log")
    rows = get_records(ws)
    return rows or []


def _iter_stalled_autoresized_candidates(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
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

        cooldown = str(row.get("Cooldown Notes", "")).lower()
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
        amount_usd = patched.get("amount_usd") or patched.get("amount") or row.get("Amount_USD")
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
            warn(f"[stalled_autotrader] incomplete intent fields for row: token={token!r}, quote={quote!r}")
            continue

        intent = {
            "token": token,
            "venue": venue,
            "quote": quote,
            "symbol": symbol,
            "amount_usd": amount_usd_f,
            "price_usd": price_usd_f,
            "action": "BUY",
        }

        yield row, intent


def run_stalled_autotrader_shadow() -> None:
    """
    Shadow-mode driver: evaluates stalled-asset auto_resized decisions
    through trade_guard, logs what WOULD be sent to Outbox, but does NOT
    actually enqueue any commands yet.
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

    info(f"[stalled_autotrader] scanning Policy_Log (rows={len(rows)}) "
         f"max_usd={max_usd} max_trades={max_trades}")

    taken = 0
    for row, intent in _iter_stalled_autoresized_candidates(rows):
        if taken >= max_trades:
            break

        # Enforce a hard cap at this layer too.
        if intent["amount_usd"] > max_usd:
            warn(f"[stalled_autotrader] candidate over max_usd cap "
                 f"({intent['amount_usd']} > {max_usd}); skipping.")
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
        # Later we will call an Outbox helper here when we switch to live mode.
        taken += 1

    if taken == 0:
        info("[stalled_autotrader] no eligible stalled-asset candidates this run.")
    else:
        info(f"[stalled_autotrader] evaluated {taken} stalled-asset candidate(s) (shadow mode).")
