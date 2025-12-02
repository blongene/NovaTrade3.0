# receipts_api.py — legacy /api/receipts/ack → Sheet + Postgres trades
import os, time, json, hmac, hashlib, sqlite3
from flask import Blueprint, request, jsonify
from utils import get_gspread_client, send_telegram_message_dedup  # already in your app
from db_backbone import record_trade_live  # Phase 19: mirror trades into Postgres

bp = Blueprint("receipts_api", __name__)

SHEET_URL         = os.getenv("SHEET_URL")
OUTBOX_DB_PATH    = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
SECRET            = os.getenv("OUTBOX_SECRET") or os.getenv("EDGE_SECRET")
TRADE_LOG_SHEET   = os.getenv("TRADE_LOG_SHEET", "Trade_Log")   # overrideable
HMAC_MAX_SKEW_MS  = int(os.getenv("HMAC_MAX_SKEW_MS", "300000"))  # 5m default

def _canon(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

def _hmac_ok(data: dict, sig_hex: str) -> bool:
    want = hmac.new(SECRET.encode("utf-8"), _canon(data), hashlib.sha256).hexdigest()
    return hmac.compare_digest(want, sig_hex)

def _db():
    conn = sqlite3.connect(OUTBOX_DB_PATH, timeout=5, isolation_level=None)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS sheet_receipts (
        id TEXT PRIMARY KEY,
        inserted_at TEXT
      )
    """)
    return conn

def _already_logged(conn, rid: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sheet_receipts WHERE id = ?", (rid,))
    return cur.fetchone() is not None

def _mark_logged(conn, rid: str):
    conn.execute("INSERT OR IGNORE INTO sheet_receipts (id, inserted_at) VALUES (?, ?)",
                 (rid, time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())))

@bp.route("/api/receipts/ack", methods=["POST"])
def receipts_ack():
    try:
        body = request.get_json(force=True) or {}
    except Exception:
        return jsonify(ok=False, error="bad json"), 400

    sig = body.pop("hmac", None)
    if not sig:
        return jsonify(ok=False, error="missing hmac"), 400

    if not _hmac_ok(body, sig):
        return jsonify(ok=False, error="bad hmac"), 401

    rid = str(body.get("id") or "")
    if not rid:
        return jsonify(ok=False, error="missing id"), 400

    conn = _db()
    if _already_logged(conn, rid):
        return jsonify(ok=True, id=rid, dedup=True)  # idempotent

    # Normalize fields
    symbol   = body.get("symbol","")
    side     = (body.get("side") or "").upper()
    status   = body.get("status","")
    venue    = body.get("venue","")
    agent_id = body.get("agent_id","")
    txid     = body.get("txid","")
    note     = body.get("note","")
    fills    = body.get("fills") or []  # [{"price": "...", "qty": "..."}]

    # basic rollups
    try:
        qty = sum(float(f.get("qty", 0) or 0) for f in fills)
    except Exception:
        qty = ""
    try:
        px = float(fills[0].get("price")) if fills and fills[0].get("price") not in (None,"") else ""
    except Exception:
        px = ""
    usdt = (qty * px) if (qty not in ("", None) and px not in ("", None)) else ""

    # Append to Google Sheet
    try:
        gc = get_gspread_client()
        sh = gc.open_by_url(SHEET_URL)
        ws = sh.worksheet(TRADE_LOG_SHEET)
    except Exception as e:
        send_telegram_message_dedup(f"⚠️ receipts: sheet open failed: {e}", "rcpt_sheet_open", 30)
        return jsonify(ok=False, error=f"sheet open failed: {e}"), 500

    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    row = [
        now,               # A: ts_utc
        rid,               # B: command_id
        symbol,            # C: symbol
        side,              # D: side
        qty,               # E: qty
        px,                # F: price
        usdt,              # G: notional_usdt
        status,            # H: status
        venue,             # I: venue
        agent_id,          # J: agent_id
        txid,              # K: txid/ord_id
        note,              # L: note
        "EdgeBus"          # M: source
    ]
    try:
        ws.append_row(row, value_input_option="RAW")
        _mark_logged(conn, rid)
    except Exception as e:
        send_telegram_message_dedup(f"⚠️ receipts: append failed: {e}", "rcpt_sheet_append", 30)
        return jsonify(ok=False, error=f"append failed: {e}"), 500

    # Mirror into Postgres trades (Phase 19) — best-effort
    try:
        trade_payload = {
            "id": rid,
            "agent_id": agent_id,
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "status": status,
            "txid": txid,
            "fills": fills,
            "note": note,
        }
        record_trade_live(rid, trade_payload)
    except Exception:
        pass

    return jsonify(ok=True, id=rid)
