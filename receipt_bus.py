# receipt_bus.py — Receipts API with provenance → Google Sheet + Postgres trades
from __future__ import annotations
import os, json, time
from typing import Any, Dict, Tuple
from flask import Blueprint, request, jsonify

from hmac_auth import require_hmac
from utils import get_gspread_client  # you already use this elsewhere
from db_backbone import record_trade_live  # Phase 19: mirror trades into Postgres

bp = Blueprint("receipts_api", __name__, url_prefix="/api/receipts")

SHEET_URL = os.getenv("SHEET_URL")
TRADE_LOG_WS = os.getenv("TRADE_LOG_WS", "Trade_Log")

REQUIRE_HMAC = (os.getenv("REQUIRE_HMAC_RECEIPTS", "1").lower() in {"1","true","yes"})

def _err(status: int, msg: str):
    return jsonify({"ok": False, "error": msg}), status

def _as_json() -> Tuple[Dict[str, Any], bool]:
    try:
        return request.get_json(force=True) or {}, True
    except Exception:
        try:
            return json.loads((request.get_data() or b"{}").decode("utf-8") or "{}"), True
        except Exception:
            return {}, False

def _comp_fills(fills):
    """Return (executed_qty, avg_price) from a list of {qty,price} dicts."""
    total_qty = 0.0
    notional = 0.0
    for f in (fills or []):
        try:
            q = float(f.get("qty") or f.get("size") or 0)
            p = float(f.get("price") or 0)
            total_qty += q
            notional += q * p
        except Exception:
            continue
    avg = (notional / total_qty) if total_qty > 0 else 0.0
    return total_qty, avg

def _compact_post_balances(pb: dict | None) -> str:
    """Turn {"USDT": 18.66, "BTC": 0.0001} into 'USDT:18.66|BTC:0.0001' for a single-cell log."""
    if not isinstance(pb, dict): return ""
    try:
        items = []
        # keep headline assets first if present
        headline = ("USDT","USDC","USD","BTC","XBT","ETH")
        for k in headline:
            if k in pb:
                items.append(f"{k}:{pb[k]}")
        # append the rest (limited for brevity)
        for k,v in pb.items():
            if k in headline: continue
            items.append(f"{k}:{v}")
            if len(items) > 12: break  # avoid huge cells
        return "|".join(items)
    except Exception:
        return ""

def _append_trade_log(row: Dict[str, Any]) -> None:
    # Connect to Google Sheets and append a normalized row.
    if not SHEET_URL:
        return
    gc = get_gspread_client()
    sh = gc.open_by_url(SHEET_URL)
    ws = sh.worksheet(TRADE_LOG_WS)

    # Ensure consistent ordering; adapt to your current header set.
    # Add the three provenance columns at the end.
    values = [
        row.get("ts_iso"),
        row.get("id"),
        row.get("agent_id"),
        row.get("venue"),
        row.get("symbol"),
        row.get("side"),
        row.get("status"),
        row.get("txid"),
        row.get("executed_qty"),
        row.get("avg_price"),
        row.get("note"),
        row.get("requested_symbol"),
        row.get("resolved_symbol"),
        row.get("post_balances_compact"),
    ]
    ws.append_row(values, value_input_option="RAW")

@bp.post("/ack")
def ack():
    if REQUIRE_HMAC:
        ok, err = require_hmac(request)
        if not ok:
            return _err(401, err)

    body, ok = _as_json()
    if not ok:
        return _err(400, "malformed JSON body")

    # Expected payload shape from Edge (already live in your system)
    # {
    #   "id", "agent_id", "venue", "symbol", "side",
    #   "status", "txid", "fills", "ts", "note",
    #   "requested_symbol", "resolved_symbol", "post_balances", "hmac"
    # }
    rid   = body.get("id")
    agent = body.get("agent_id")
    venue = (body.get("venue") or "").upper()
    symbol= body.get("symbol") or ""
    side  = (body.get("side") or "").upper()
    status= (body.get("status") or "").lower()
    txid  = body.get("txid") or ""
    fills = body.get("fills") or []
    note  = body.get("note") or ""

    req_sym = body.get("requested_symbol") or ""
    res_sym = body.get("resolved_symbol") or symbol
    post_bal= body.get("post_balances") if isinstance(body.get("post_balances"), dict) else None

    qty, avg = _comp_fills(fills)
    ts_iso = body.get("ts") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Append to Trade_Log (best-effort)
    try:
        row = {
            "ts_iso": ts_iso,
            "id": rid,
            "agent_id": agent,
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "status": status,
            "txid": txid,
            "executed_qty": qty,
            "avg_price": avg,
            "note": note,
            "requested_symbol": req_sym,
            "resolved_symbol": res_sym,
            "post_balances_compact": _compact_post_balances(post_bal),
        }
        _append_trade_log(row)
    except Exception as e:
        # Non-fatal: still return OK so Edge doesn’t retry forever
        return jsonify({"ok": True, "id": str(rid), "sheet": "error", "error": str(e)}), 200

    # Mirror into Postgres trades (Phase 19) — also best-effort
    try:
        trade_payload = {
            "id": rid,
            "agent_id": agent,
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "status": status,
            "txid": txid,
            "fills": fills,
            "note": note,
            "requested_symbol": req_sym,
            "resolved_symbol": res_sym,
            "post_balances": post_bal,
        }
        record_trade_live(rid, trade_payload)
    except Exception:
        # Never break Edge acknowledgements on DB mirror issues
        pass

    return jsonify({"ok": True, "id": str(rid)}), 200
