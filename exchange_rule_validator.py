#!/usr/bin/env python3
"""
exchange_rule_validator.py — NovaTrade 3.0

Purpose
-------
Centralised "pre-flight" exchange rules that run *before* we enqueue
a command to the Edge Agent.

These rules sit below the high-level Policy Engine:
- Policy Engine answers "Is it sane for the portfolio?"
- ExchangeRuleValidator answers "Is this order even legal for the venue?"

Current focus (Pin 5 scope):
  * Venue/token whitelist (avoid "Unknown asset pair" errors)
  * Venue-level min notional (avoid "min notional 10 USDT not met" errors)

The validator is intentionally conservative. It will:
  - Block tokens we haven't explicitly whitelisted per venue
  - Block orders below the configured min_notional per venue

If the rules fail, we return ok=False and a clear, human-readable reason.
That reason flows into Policy_Log, NovaTrigger_Log and Trade_Log.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

import os

from utils import info, warn


@dataclass
class RuleResult:
    ok: bool
    reason: str
    patched_intent: Dict[str, Any]


# ---------------------------------------------------------------------------
# Static rule tables
# ---------------------------------------------------------------------------

# Venue-level min notional, expressed in *USD/USDT* terms.
# These are intentionally conservative; we’d rather under-trade than get
# exchange errors.
_MIN_NOTIONAL_DEFAULTS = {
    "BINANCEUS": 10.0,  # observed "min notional 10 USDT not met"
    "KRAKEN": 10.0,     # Kraken spot typical min notionals in that range
    "COINBASE": 1.0,    # Coinbase is more permissive; 1 USD is safe
}

# Very small epsilon to avoid float flakiness when checking thresholds
_EPS = 1e-9

# Token whitelist per venue.
# For now we intentionally whitelist *only* the pairs we’re comfortable
# auto-trading. Anything else will be blocked with a clear reason.
#
# If you want to enable more assets later, extend these sets.
_TOKEN_WHITELIST_DEFAULTS = {
    # Core "safe" BTC spot pairs
    "BINANCEUS": {"BTC"},   # BTC/USDT, BTC/USD via executor mapping
    "KRAKEN": {"BTC"},      # BTC/USDT, BTC/USD
    "COINBASE": {"BTC"},    # BTC/USD
}

# ---------------------------------------------------------------------------
# Helpers to allow env-based tuning without code changes
# ---------------------------------------------------------------------------


def _build_min_notional_table() -> Dict[str, float]:
    """
    Allow per-venue override via env:

        EX_RULES_MIN_NOTIONAL_BINANCEUS=12.5
        EX_RULES_MIN_NOTIONAL_KRAKEN=5

    If unset, we fall back to _MIN_NOTIONAL_DEFAULTS.
    """
    out: Dict[str, float] = dict(_MIN_NOTIONAL_DEFAULTS)
    for venue in list(_MIN_NOTIONAL_DEFAULTS.keys()):
        env_name = f"EX_RULES_MIN_NOTIONAL_{venue}"
        raw = os.getenv(env_name)
        if not raw:
            continue
        try:
            value = float(raw)
            out[venue] = value
            info(
                f"exchange_rules: override {env_name}={value:g} "
                f"(was {_MIN_NOTIONAL_DEFAULTS[venue]:g})"
            )
        except Exception:
            warn(f"exchange_rules: bad float for {env_name}={raw!r}; ignoring override")
    return out


_MIN_NOTIONAL = _build_min_notional_table()


def _normalise_venue(venue: str) -> str:
    return (venue or "").strip().upper()


def _normalise_token(token: str) -> str:
    return (token or "").strip().upper()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def apply_exchange_rules(intent: Dict[str, Any]) -> RuleResult:
    """
    Apply venue/token/min_notional exchange rules to a *patched* intent.

    Expected intent shape (subset):
        {
            "venue": "BINANCEUS" | "KRAKEN" | "COINBASE" | ...,
            "token": "BTC",
            "quote": "USDT" | "USD" | ...,
            "amount_usd": 47.61,
            ... (other fields ignored by this validator)
        }

    Returns:
        RuleResult(ok, reason, patched_intent)

    Behaviour:
      - If venue is unknown to this module → we log a warning and fail OPEN
        (ok=True) rather than blocking.
      - If token is not whitelisted for venue → ok=False.
      - If amount_usd < venue min_notional → ok=False.
      - Otherwise ok=True.
    """
    if not isinstance(intent, dict):
        return RuleResult(
            ok=False,
            reason="exchange_rules: invalid intent (not a dict)",
            patched_intent=intent,
        )

    venue = _normalise_venue(intent.get("venue", ""))
    token = _normalise_token(intent.get("token", ""))
    quote = (intent.get("quote") or "").upper() or "USD"

    # Fail-open for unknown venues: let the executor decide rather than
    # surprise-blocking entirely new exchanges.
    if venue not in _MIN_NOTIONAL:
        warn(f"exchange_rules: unknown venue {venue!r}; skipping exchange rules.")
        return RuleResult(ok=True, reason="exchange_rules: skipped (unknown venue)", patched_intent=intent)

    # 1) Token whitelist
    allowed_tokens = _TOKEN_WHITELIST_DEFAULTS.get(venue)
    if allowed_tokens and token and token not in allowed_tokens:
        reason = (
            f"exchange_rules: unsupported token for {venue}: {token}; "
            f"allowed={sorted(allowed_tokens)}"
        )
        return RuleResult(ok=False, reason=reason, patched_intent=intent)

    # 2) Min notional check (in USD/USDT terms)
    amount_usd_raw = intent.get("amount_usd", 0.0)
    try:
        amount_usd = float(amount_usd_raw or 0.0)
    except Exception:
        return RuleResult(
            ok=False,
            reason=f"exchange_rules: invalid amount_usd={amount_usd_raw!r}",
            patched_intent=intent,
        )

    min_notional = _MIN_NOTIONAL.get(venue)
    if min_notional is not None and amount_usd + _EPS < min_notional:
        reason = (
            f"exchange_rules: min_notional {min_notional:g} {quote} not met "
            f"(requested {amount_usd:g} {quote})"
        )
        return RuleResult(ok=False, reason=reason, patched_intent=intent)

    # If we get here, all rules passed.
    return RuleResult(
        ok=True,
        reason="exchange_rules: ok",
        patched_intent=intent,
    )
