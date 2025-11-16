"""
price_feed.py — B-2 price oracle for NovaTrade

Main entrypoint:

    get_price_usd(token: str, quote: str = "USDT", venue: str | None = None) -> float | None

Behavior:
    • Try venue-specific public APIs first (no auth):
        - BINANCEUS  -> https://api.binance.us/api/v3/ticker/price
        - COINBASE   -> https://api.exchange.coinbase.com/products/{BASE}-{QUOTE}/ticker
        - KRAKEN     -> https://api.kraken.com/0/public/Ticker
    • If venue is None or unsupported, try BinanceUS BTC/USDT-style symbol.
    • Cache responses for PRICE_FEED_TTL_SEC (default 30s) to avoid hammering APIs.
    • Return None on failure; callers (trade_guard/PolicyEngine) will deny or fall back.
"""

from __future__ import annotations

import os
import time
from typing import Dict, Tuple, Optional

import requests  # type: ignore
from utils import warn  # type: ignore

PRICE_FEED_TTL_SEC = int(os.getenv("PRICE_FEED_TTL_SEC", "30"))

# (venue, token, quote) -> (price, ts)
_price_cache: Dict[Tuple[str, str, str], Tuple[float, float]] = {}


def _cache_get(venue: str, token: str, quote: str) -> Optional[float]:
    key = (venue.upper(), token.upper(), quote.upper())
    val = _price_cache.get(key)
    if not val:
        return None
    price, ts = val
    if time.time() - ts > PRICE_FEED_TTL_SEC:
        return None
    return price


def _cache_set(venue: str, token: str, quote: str, price: float) -> None:
    key = (venue.upper(), token.upper(), quote.upper())
    _price_cache[key] = (float(price), time.time())


def _fetch_binanceus(base: str, quote: str) -> Optional[float]:
    """
    Binance.US public ticker:
        GET /api/v3/ticker/price?symbol=BTCUSDT
    """
    symbol = f"{base}{quote}".upper()
    url = "https://api.binance.us/api/v3/ticker/price"
    try:
        r = requests.get(url, params={"symbol": symbol}, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        price = float(data["price"])
        return price if price > 0 else None
    except Exception as e:
        warn(f"price_feed: binanceus error for {symbol}: {e}")
        return None


def _fetch_coinbase(base: str, quote: str) -> Optional[float]:
    """
    Coinbase public ticker:
        GET /products/BTC-USD/ticker
    """
    product = f"{base}-{quote}".upper()
    url = f"https://api.exchange.coinbase.com/products/{product}/ticker"
    try:
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        price = float(data.get("price") or data.get("last"))
        return price if price > 0 else None
    except Exception as e:
        warn(f"price_feed: coinbase error for {product}: {e}")
        return None


def _kraken_pair(base: str, quote: str) -> str:
    """
    Very small helper to map BTC/ETH to Kraken's XBT/ETH pairs.
    We keep this simple for now and extend as needed.
    """
    b = base.upper()
    q = quote.upper()
    if b == "BTC":
        b = "XBT"
    if q == "BTC":
        q = "XBT"
    return f"{b}{q}"


def _fetch_kraken(base: str, quote: str) -> Optional[float]:
    """
    Kraken public ticker:
        GET /0/public/Ticker?pair=XBTUSD
    """
    pair = _kraken_pair(base, quote)
    url = "https://api.kraken.com/0/public/Ticker"
    try:
        r = requests.get(url, params={"pair": pair}, timeout=5)
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("error"):
            return None
        result = data.get("result") or {}
        if not result:
            return None
        # Get first entry
        ticker = next(iter(result.values()))
        # 'c' is last trade [price, volume]
        price = float(ticker["c"][0])
        return price if price > 0 else None
    except Exception as e:
        warn(f"price_feed: kraken error for {pair}: {e}")
        return None


def _fetch_generic(base: str, quote: str) -> Optional[float]:
    """
    Simple generic fallback: try BinanceUS as a global feed even if venue is unknown.
    """
    return _fetch_binanceus(base, quote)


def get_price_usd(token: str, quote: str = "USDT", venue: Optional[str] = None) -> Optional[float]:
    """
    Main B-2 price oracle entrypoint.

    Args:
        token: base asset symbol, e.g. "BTC"
        quote: quote asset symbol, e.g. "USDT" or "USD"
        venue: preferred venue ("BINANCEUS", "COINBASE", "KRAKEN") or None

    Returns:
        float price in quote units (usually USD/USDT) or None if unavailable.
    """
    token = (token or "").upper()
    quote = (quote or "USDT").upper()
    venue = (venue or "").upper()

    if not token:
        return None

    # Identity: if token itself is a USD stable, price≈1.
    if token in ("USDT", "USDC", "USD") and quote in ("USDT", "USDC", "USD"):
        return 1.0

    # Cache
    cached = _cache_get(venue or "ANY", token, quote)
    if cached is not None:
        return cached

    price: Optional[float] = None

    # Venue-specific first
    if venue == "BINANCEUS":
        price = _fetch_binanceus(token, quote)
    elif venue == "COINBASE":
        price = _fetch_coinbase(token, quote)
    elif venue == "KRAKEN":
        price = _fetch_kraken(token, quote)

    # Fallback if venue unsupported or failed
    if price is None:
        price = _fetch_generic(token, quote)

    if price is not None:
        _cache_set(venue or "ANY", token, quote, price)

    return price


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python price_feed.py BTC [USDT] [VENUE]")
        raise SystemExit(1)

    t = sys.argv[1]
    q = sys.argv[2] if len(sys.argv) > 2 else "USDT"
    v = sys.argv[3] if len(sys.argv) > 3 else None

    p = get_price_usd(t, q, v)
    print(f"Price {t}/{q} @ {v or 'ANY'} = {p}")
