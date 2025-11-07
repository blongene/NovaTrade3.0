# smoke_router.py — quick sanity checks (no network)
from router import choose_venue
import time

policy = {
    "keepback_usd": 5,
    "min_quote_reserve_usd": 10,
    "canary_max_usd": 11,
    "max_per_coin_usd": 25,
    "prefer_quotes": {"BINANCEUS":"USDT", "COINBASE":"USDC", "KRAKEN":"USDT"},
    "venue_order": ["BINANCEUS","COINBASE","KRAKEN"],
    "telemetry_max_age_sec": 600,
}

telemetry = {
    "ts": int(time.time()),
    "by_venue": {
        "BINANCEUS": {"USDT": 450.0},
        "COINBASE":  {"USDC": 959.0},
        "KRAKEN":    {"USDT": 19.52, "USDC": 0.00005},
    }
}

intent = {"symbol":"BTC-USD","side":"buy","amount":0.005,"price_usd":103000}
print(choose_venue(intent, telemetry, policy))
# Expect BINANCEUS, amount bumped to >= min-notional, flags include clamped/min_notional_bump/prefer_quote (if needed)
