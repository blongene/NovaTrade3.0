# ops_venue.py — venue availability helper
import json
from flask import Blueprint, request, jsonify

bp = Blueprint("ops_venue", __name__, url_prefix="/ops")

# Very simple map (extend later by fetching each venue's /exchangeInfo or product list)
# Keys must be normalized to your payload.symbol format "BASE/QUOTE"
SUPPORTED = {
    "COINBASE": {"BTC/USDT","ETH/USDT","SOL/USDT","BTC/USD","ETH/USD"},
    "BINANCE.US": {"BTC/USDT","ETH/USDT","SOL/USDT"},
    "MEXC": {"BTC/USDT","ETH/USDT","SOL/USDT"},
    "KRAKEN": {"BTC/USDT","ETH/USDT"}  # placeholder
}

def normalize(s: str) -> str:
    return s.upper()

@bp.get("/venue_check")
def venue_check():
    symbol = request.args.get("symbol","").strip()
    if not symbol:
        return jsonify(ok=False, error="missing symbol"), 400
    sym = normalize(symbol)
    out = []
    for venue, pairs in SUPPORTED.items():
        if sym in pairs:
            out.append(venue)
    return jsonify(ok=True, symbol=sym, venues=sorted(out))
