import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests

from policy_engine import evaluate_manual_rebuy
from utils import (
    get_env_bool,
    get_env_int,
    get_env_str,
    get_sheet_client,
    retry_on_exception,
    sheets_append_rows,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SPREADSHEET_URL = os.environ.get("SHEET_URL")
TAB_NOVA_TRIGGER = os.environ.get("NOVA_TRIGGER_TAB", "NovaTrigger")
TAB_POLICY_LOG = os.environ.get("POLICY_LOG_TAB", "Policy_Log")
TAB_TRADE_LOG = os.environ.get("TRADE_LOG_TAB", "Trade_Log")

# How much jitter to apply when checking NovaTrigger via CLI / scripts
NOVA_TRIGGER_JITTER_MIN_S = get_env_int("NOVA_TRIGGER_JITTER_MIN_S", 2)
NOVA_TRIGGER_JITTER_MAX_S = get_env_int("NOVA_TRIGGER_JITTER_MAX_S", 6)

# Policy toggles
ALLOW_BINANCEUS = get_env_bool("ALLOW_BINANCEUS", True)
ALLOW_COINBASE = get_env_bool("ALLOW_COINBASE", True)
ALLOW_KRAKEN = get_env_bool("ALLOW_KRAKEN", True)

# Manual-rebuy sizing policy knobs
DEFAULT_QUOTE_SIZE_USD = float(os.environ.get("DEFAULT_QUOTE_SIZE_USD", "10"))
MIN_QUOTE_SIZE_USD = float(os.environ.get("MIN_QUOTE_SIZE_USD", "10"))
MAX_QUOTE_SIZE_USD = float(os.environ.get("MAX_QUOTE_SIZE_USD", "250"))

# Price fetch
PRICE_API_TIMEOUT_S = float(os.environ.get("PRICE_API_TIMEOUT_S", "10"))

BINANCE_US_PRICE_URL = "https://api.binance.us/api/v3/ticker/price"
COINBASE_PRICE_URL = "https://api.exchange.coinbase.com/products/{symbol}/ticker"
KRAKEN_PRICE_URL = "https://api.kraken.com/0/public/Ticker"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class ManualRebuyIntent:
    token: str
    venue: str
    amount_usd: float
    raw: str


def _open_sheet():
    gc = get_sheet_client()
    return gc.open_by_url(SPREADSHEET_URL)


def _get_worksheet(tab_name: str):
    sh = _open_sheet()
    return sh.worksheet(tab_name)


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Price fetchers
# ---------------------------------------------------------------------------


def _fetch_binance_us_price(symbol: str) -> float:
    """
    Fetch last price from Binance.US.

    symbol should be something like "BTCUSDT" or "ETHUSDT".
    """
    params = {"symbol": symbol}
    resp = requests.get(BINANCE_US_PRICE_URL, params=params, timeout=PRICE_API_TIMEOUT_S)
    resp.raise_for_status()
    data = resp.json()
    return _safe_float(data.get("price"))


def _fetch_coinbase_price(symbol: str) -> float:
    """
    Fetch last price from Coinbase Advanced.

    symbol should be like "BTC-USD".
    """
    url = COINBASE_PRICE_URL.format(symbol=symbol)
    resp = requests.get(url, timeout=PRICE_API_TIMEOUT_S)
    resp.raise_for_status()
    data = resp.json()
    return _safe_float(data.get("price"))


def _fetch_kraken_price(symbol: str) -> float:
    """
    Fetch last price from Kraken.

    symbol should be like "XBTUSD", "ETHUSD", etc.
    """
    params = {"pair": symbol}
    resp = requests.get(KRAKEN_PRICE_URL, params=params, timeout=PRICE_API_TIMEOUT_S)
    resp.raise_for_status()
    data = resp.json()
    result = data.get("result") or {}
    if not result:
        return 0.0
    first_key = next(iter(result.keys()))
    ticker = result[first_key]
    # Kraken last trade price is "c"[0]
    return _safe_float((ticker.get("c") or [0])[0])


@retry_on_exception(
    retries=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(requests.RequestException,),
    label="price fetch",
)
def _fetch_price(venue: str, symbol: str) -> float:
    venue = venue.upper()
    if venue == "BINANCEUS":
        return _fetch_binance_us_price(symbol)
    if venue == "COINBASE":
        return _fetch_coinbase_price(symbol)
    if venue == "KRAKEN":
        return _fetch_kraken_price(symbol)
    raise ValueError(f"Unsupported venue for price fetch: {venue}")


# ---------------------------------------------------------------------------
# Parsing manual intents
# ---------------------------------------------------------------------------


def _parse_manual_rebuy(raw: str) -> ManualRebuyIntent:
    """
    Parse lines like:

      MANUAL_REBUY BTC 5 VENUE=BINANCEUS
      MANUAL_REBUY BTC 25 VENUE=COINBASE
      MANUAL_REBUY OCEAN 15 VENUE=KRAKEN

    If the quote size is omitted, DEFAULT_QUOTE_SIZE_USD is used.
    """
    parts = raw.strip().split()
    # MANUAL_REBUY TOKEN [size] VENUE=XXX
    if len(parts) < 3:
        raise ValueError("Expected: MANUAL_REBUY <TOKEN> <SIZE?> VENUE=<VENUE>")

    if parts[0].upper() != "MANUAL_REBUY":
        raise ValueError("Line does not start with MANUAL_REBUY")

    token = parts[1].upper()

    # look for VENUE=*
    venue = None
    amount_usd = DEFAULT_QUOTE_SIZE_USD
    for p in parts[2:]:
        up = p.upper()
        if up.startswith("VENUE="):
            venue = up.split("=", 1)[1]
        else:
            # try to parse as numeric size
            maybe = _safe_float(p, float("nan"))
            if not (maybe != maybe):  # not NaN
                amount_usd = maybe

    if not venue:
        raise ValueError("Missing VENUE=<VENUE> in MANUAL_REBUY line")

    return ManualRebuyIntent(token=token, venue=venue, amount_usd=amount_usd, raw=raw)


# ---------------------------------------------------------------------------
# Symbol normalization per venue
# ---------------------------------------------------------------------------


def _normalize_symbol_for_venue(token: str, venue: str) -> Tuple[str, str]:
    """
    Return (requested_symbol, resolved_symbol) for a given token+venue.

    requested_symbol is what the user conceptually means ("BTCUSDT", "BTC-USD",
    etc.), resolved_symbol is what we actually send to the executor. These can
    be the same, but having both helps the Trade_Log stay readable.
    """
    t = token.upper()
    v = venue.upper()

    if v == "BINANCEUS":
        # Binance.US generally uses USDT or USD pairs in "BTCUSDT" style.
        # For now we hard-code BTCUSDT; we can expand for more tokens later.
        return "BTCUSDT", "BTCUSDT"

    if v == "COINBASE":
        # Coinbase advanced uses "BTC-USD" style product IDs.
        return "BTC-USD", "BTC-USD"

    if v == "KRAKEN":
        # Kraken spot often uses XBTUSD for BTC/USD.
        return "XBTUSD", "XBTUSD"

    return f"{t}USD", f"{t}USD"


# ---------------------------------------------------------------------------
# Policy / logging helpers
# ---------------------------------------------------------------------------


def _append_policy_log_row(
    ws,
    action: str,
    token: str,
    amount_usd: float,
    ok: bool,
    reason: str,
    patched: Dict[str, Any],
    venue: str,
    quote_symbol: str,
    liquidity_note: str,
    cooldown_note: str,
    intent_id: str,
    symbol: str,
    decision: Dict[str, Any],
):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = [
        ts,
        token,
        action,
        amount_usd,
        "TRUE" if ok else "FALSE",
        reason,
        json.dumps(patched, separators=(",", ":")),
        venue,
        quote_symbol,
        liquidity_note,
        cooldown_note,
        intent_id,
        symbol,
        json.dumps(decision, separators=(",", ":")),
    ]
    sheets_append_rows(ws, [row])


def _append_trade_log_row(
    ws,
    venue: str,
    requested_symbol: str,
    resolved_symbol: str,
    side: str,
    amount_quote: float,
    executed_qty: float,
    avg_price: float,
    status: str,
    notes: str,
    cmd_id: int,
    receipt_id: int,
    note_id: str,
):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = [
        ts,
        venue,
        requested_symbol,
        side,
        amount_quote,
        executed_qty,
        avg_price,
        status,
        notes,
        cmd_id,
        receipt_id,
        note_id,
        "EdgeBus",
        requested_symbol,
        resolved_symbol,
    ]
    sheets_append_rows(ws, [row])


# ---------------------------------------------------------------------------
# Main routing entrypoint
# ---------------------------------------------------------------------------


def route_manual(raw: str) -> Dict[str, Any]:
    """
    Main entrypoint used by NovaTriggerWatcher to route manual commands.

    Returns a dict with "decision" (policy decision) and "enqueue" (command
    enqueue result from the Bus, if any).
    """
    try:
        intent = _parse_manual_rebuy(raw)
    except Exception as e:
        print(f"⚠ Failed to parse MANUAL_REBUY line: {e}")
        return {
            "decision": {"ok": False, "reason": f"parse_error: {e}"},
            "enqueue": {"ok": False, "reason": "parse_error"},
        }

    # Venue toggles
    v = intent.venue.upper()
    if v == "BINANCEUS" and not ALLOW_BINANCEUS:
        return {
            "decision": {
                "ok": False,
                "reason": "venue_binanceus_disabled",
            },
            "enqueue": {"ok": False, "reason": "venue_binanceus_disabled"},
        }
    if v == "COINBASE" and not ALLOW_COINBASE:
        return {
            "decision": {
                "ok": False,
                "reason": "venue_coinbase_disabled",
            },
            "enqueue": {"ok": False, "reason": "venue_coinbase_disabled"},
        }
    if v == "KRAKEN" and not ALLOW_KRAKEN:
        return {
            "decision": {
                "ok": False,
                "reason": "venue_kraken_disabled",
            },
            "enqueue": {"ok": False, "reason": "venue_kraken_disabled"},
        }

    if intent.amount_usd < MIN_QUOTE_SIZE_USD:
        return {
            "decision": {
                "ok": False,
                "reason": f"quote below min reserve (${MIN_QUOTE_SIZE_USD:.2f} < ${intent.amount_usd:.2f})",
            },
            "enqueue": {"ok": False, "reason": "quote below min reserve"},
        }

    if intent.amount_usd > MAX_QUOTE_SIZE_USD:
        # clamp down
        intent.amount_usd = MAX_QUOTE_SIZE_USD

    # Normalize symbol & fetch price
    requested_symbol, resolved_symbol = _normalize_symbol_for_venue(
        intent.token, intent.venue
    )

    try:
        price = _fetch_price(intent.venue, requested_symbol)
    except Exception as e:
        print(f"⚠ price unknown; sizing requires price: {e}")
        decision = {
            "ok": False,
            "reason": "price_unknown; sizing requires price",
        }
        enqueue_result = {"ok": False, "reason": "price_unknown"}
        _log_policy_only(intent, decision, enqueue_result)
        return {"decision": decision, "enqueue": enqueue_result}

    if price <= 0:
        decision = {
            "ok": False,
            "reason": "price_unknown; non-positive price",
        }
        enqueue_result = {"ok": False, "reason": "price_unknown"}
        _log_policy_only(intent, decision, enqueue_result)
        return {"decision": decision, "enqueue": enqueue_result}

    # Convert USD size to base quantity
    size_quote = intent.amount_usd
    size_base = size_quote / price

    # Hand to policy engine
    decision = evaluate_manual_rebuy(
        token=intent.token,
        venue=intent.venue,
        quote_size=size_quote,
        base_size=size_base,
        price=price,
    )

    # decision: {ok: bool, reason: str, patched_intent: {...}, cooldown, liquidity, etc.}
    if not isinstance(decision, dict):
        decision = {"ok": False, "reason": "policy_engine_invalid_response"}

    patched = decision.get("patched_intent") or {}
    action = "manual_rebuy"

    # Logging to Policy_Log
    try:
        ws_policy = _get_worksheet(TAB_POLICY_LOG)
    except Exception as e:
        print(f"⚠ Failed to open Policy_Log sheet: {e}")
        ws_policy = None

    if ws_policy is not None:
        try:
            _append_policy_log_row(
                ws_policy,
                action=action,
                token=intent.token,
                amount_usd=size_quote,
                ok=bool(decision.get("ok")),
                reason=decision.get("reason", "ok"),
                patched=patched,
                venue=intent.venue,
                quote_symbol="USDT",
                liquidity_note=decision.get("liquidity_note", ""),
                cooldown_note=decision.get("cooldown_note", ""),
                intent_id=decision.get("intent_id", ""),
                symbol=requested_symbol,
                decision=decision,
            )
        except Exception as e:
            print(f"⚠ Failed to append Policy_Log row: {e}")

    # If policy rejected, stop here
    if not decision.get("ok"):
        enqueue_result = {"ok": False, "reason": decision.get("reason", "policy_rejected")}
        return {"decision": decision, "enqueue": enqueue_result}

    # Otherwise enqueue a command into the Bus via Ops/Commands API
    try:
        from ops_intents import enqueue_manual_rebuy  # lazy import to avoid cycles

        enqueue_result = enqueue_manual_rebuy(
            venue=intent.venue,
            symbol=requested_symbol,
            quote_amount=size_quote,
            base_amount=size_base,
            price=price,
            decision=decision,
        )
    except Exception as e:
        print(f"⚠ enqueue failed: {e}")
        enqueue_result = {"ok": False, "reason": f"enqueue_failed: {e}"}

    return {"decision": decision, "enqueue": enqueue_result}


def _log_policy_only(
    intent: ManualRebuyIntent,
    decision: Dict[str, Any],
    enqueue: Dict[str, Any],
) -> None:
    """Helper for failure paths where we still want a Policy_Log row."""
    try:
        ws_policy = _get_worksheet(TAB_POLICY_LOG)
    except Exception as e:
        print(f"⚠ Failed to open Policy_Log sheet: {e}")
        return

    try:
        _append_policy_log_row(
            ws_policy,
            action="manual_rebuy",
            token=intent.token,
            amount_usd=intent.amount_usd,
            ok=bool(decision.get("ok")),
            reason=decision.get("reason", "ok"),
            patched=decision.get("patched_intent") or {},
            venue=intent.venue,
            quote_symbol="USDT",
            liquidity_note=decision.get("liquidity_note", ""),
            cooldown_note=decision.get("cooldown_note", ""),
            intent_id=decision.get("intent_id", ""),
            symbol=intent.token,
            decision=decision,
        )
    except Exception as e:
        print(f"⚠ Failed to append Policy_Log row (policy-only): {e}")


if __name__ == "__main__":
    # Simple CLI helper for local tests
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m nova_trigger 'MANUAL_REBUY BTC 10 VENUE=BINANCEUS'")
        sys.exit(1)

    raw_line = sys.argv[1]
    print(f"Raw: {raw_line}")
    out = route_manual(raw_line)
    print(json.dumps(out, indent=2, sort_keys=True))
