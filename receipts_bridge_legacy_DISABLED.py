# receipts_bridge.py — mirror DB receipts into Google Sheets (Trade_Log)
# CLEAN • LEAN • QUIET • EFFICIENT
#
# Env:
#   OUTBOX_DB_PATH=/data/outbox.db
#   SHEET_URL=<your tracker>
#   TRADE_LOG_WS=Trade_Log                (optional; default Trade_Log)
#   RECEIPTS_BRIDGE_STATE=/data/receipts_bridge.state
#   BRIDGE_MAX_PER_TICK=200               (optional; max receipts processed in one run)
#   BRIDGE_BATCH_SIZE=100                 (optional; rows per Sheets append)
#   BRIDGE_MIN_INTERVAL_SEC=300           (optional; scheduler handles cadence; this is just a guard)
#
# Sheets auth:
#   Uses utils.sheets_append_rows(), which supports:
#     - GSPREAD_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_JSON (inline JSON)
#     - GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/service_account.json
#     - fallbacks (/etc/secrets/service_account.json, /opt/render/.config/gspread/service_account.json)

from __future__ import annotations
import os, sqlite3, json, time, sys, traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

# Our utils provides rate-limited, budgeted appends + auth fallbacks
from utils import sheets_append_rows
from db_backbone import record_trade_live  # Phase 19: mirror trades into Postgres

# ----------- config ----------
DB_PATH                = os.getenv("OUTBOX_DB_PATH", "/data/outbox.db")
SHEET_URL              = os.getenv("SHEET_URL", "")
TRADE_LOG_WS           = os.getenv("TRADE_LOG_WS", "Trade_Log")
STATE_PATH             = os.getenv("RECEIPTS_BRIDGE_STATE", "/data/receipts_bridge.state")
MAX_PER_TICK           = int(os.getenv("BRIDGE_MAX_PER_TICK", "200"))
BATCH_SIZE             = int(os.getenv("BRIDGE_BATCH_SIZE", "100"))
MIN_INTERVAL_SEC       = int(os.getenv("BRIDGE_MIN_INTERVAL_SEC", "300"))  # guard; scheduler is source of truth
ROW_SHAPE = [
    "Timestamp","Venue","Symbol","Side","Amount_Quote","Executed_Qty","Avg_Price","Status",
    "Notes","Cmd_ID","Receipt_ID","Note","Source","requested_symbol","resolved_symbol","post_balances_compact"
]
SOURCE_LABEL = "EdgeBus"

# ----------- helpers ----------

def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def _parse_iso(ts: Any) -> Optional[datetime]:
    if ts is None: return None
    try:
        s = str(ts)
        if not s: return None
        # Accept "Z"
        s = s.replace("Z", "+00:00")
        # Accept unix seconds
        if s.isdigit():
            return datetime.fromtimestamp(int(s), tz=timezone=timezone.utc)
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _read_state() -> Tuple[int, float]:
    """Returns (last_id, last_run_epoch)."""
    try:
        with open(STATE_PATH, "r") as f:
            raw = f.read().strip()
        if not raw:
            return 0, 0.0
        parts = raw.split(",")
        last_id = int(parts[0]) if parts else 0
        last_run = float(parts[1]) if len(parts) > 1 else 0.0
        return last_id, last_run
    except Exception:
        return 0, 0.0

def _write_state(last_id: int) -> None:
    tmp = STATE_PATH + ".tmp"
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(tmp, "w") as f:
        f.write(f"{last_id},{time.time():.3f}")
    os.replace(tmp, STATE_PATH)

def _connect_sqlite() -> sqlite3.Connection:
    # generous timeout for locked db, read-only pattern
    return sqlite3.connect(DB_PATH, timeout=5, check_same_thread=False)

def _fetch_new_receipts(after_id: int, limit: int) -> List[Dict[str, Any]]:
    """Returns list of {id, ts, payload} dicts."""
    try:
        con = _connect_sqlite()
        cur = con.cursor()
        # receipts schema: id, payload, ts (ts may not exist on very old builds -> handle)
        try:
            cur.execute("SELECT id, ts, payload FROM receipts WHERE id > ? ORDER BY id ASC LIMIT ?", (after_id, limit))
            rows = cur.fetchall()
        except sqlite3.OperationalError:
            # very old schema without ts
            cur.execute("SELECT id, payload FROM receipts WHERE id > ? ORDER BY id ASC LIMIT ?", (after_id, limit))
            rows = [(r[0], None, r[1]) for r in cur.fetchall()]
        finally:
            con.close()
        items = [{"id": r[0], "ts": r[1], "payload": r[2]} for r in rows]
        return items
    except Exception as e:
        print(f"[bridge] fetch error: {e}")
        traceback.print_exc()
        return []

def _is_ack_only(j: Dict[str, Any]) -> bool:
    """True if this is an ACK mirror with no trade metadata (don’t write)."""
    rec = j.get("receipt") or {}
    cmd = j.get("command") or {}
    venue  = rec.get("venue") or cmd.get("venue")
    symbol = rec.get("symbol") or cmd.get("symbol") or cmd.get("pair")
    side   = rec.get("side")   or cmd.get("side")
    note   = rec.get("note") or rec.get("message") or rec.get("error") or ""
    status = (rec.get("status") or j.get("status") or "").lower()
    minimal_fields = not (venue or symbol or side)
    return minimal_fields and status in {"ok","held","error"} and not note

def _build_row(rcp_id: int, j: Dict[str, Any]) -> Optional[List[Any]]:
    """Build one Trade_Log row from a receipt payload. Return None if we should skip."""
    if _is_ack_only(j):
        return None

    rec  = j.get("receipt") or {}
    cmd  = j.get("command") or {}
    meta = j.get("meta")    or {}

    # timestamp preference: receipt.ts → receipt.timestamp → meta.ts → cmd.ts → now (NOT epoch)
    ts = rec.get("ts") or rec.get("timestamp") or meta.get("ts") or cmd.get("ts")
    dt = _parse_iso(ts) or datetime.now(timezone.utc)
    ts_str = dt.strftime("%Y-%m-%d %H:%M:%S")

    venue   = rec.get("venue") or cmd.get("venue") or ""
    symbol  = rec.get("symbol") or cmd.get("symbol") or cmd.get("pair") or ""
    side    = rec.get("side") or cmd.get("side") or ""
    status  = rec.get("status") or j.get("status") or ""
    # textual notes / reasons
    note = rec.get("note") or rec.get("message") or rec.get("error") or ""
    did = (cmd.get("decision_id") or meta.get("decision_id") or "")
    if did and "decision_id=" not in str(note):
        note = (str(note) + " " if note else "") + f"decision_id={did}"
    # amounts
    amt_q   = rec.get("amount_quote") or cmd.get("amount_quote") or cmd.get("amount_usd") or ""
    exec_q  = rec.get("executed_qty") or ""
    avg_px  = rec.get("avg_price") or ""

    cmd_id  = j.get("cmd_id") or meta.get("cmd_id") or cmd.get("id") or j.get("id") or ""
    rcpid   = rcp_id
    rq_sym  = cmd.get("symbol") or cmd.get("pair") or ""
    rs_sym  = rec.get("resolved_symbol") or rec.get("symbol") or ""
    post_bal = rec.get("post_balances_compact") or rec.get("post_balances") or ""

    # Produce full, stable row in your column order
    return [
      ts_str, venue, symbol, side, amt_q, exec_q, avg_px, status,
      notes, cmd_id, rcpid, notes, SOURCE_LABEL, rq_sym, rs_sym, post_bal
    ]

def _append_rows(rows: List[List[Any]]) -> int:
    if not rows:
        return 0
    if not SHEET_URL:
        print("[bridge] SHEET_URL missing; skip write")
        return 0
    # User-entered mode keeps numbers/dates friendly (handled inside utils.sheets_append_rows)
    sheets_append_rows(SHEET_URL, TRADE_LOG_WS, rows)
    return len(rows)

def _mirror_trade_to_db(rcp_id: int, j: Dict[str, Any]) -> None:
    """
    Normalize this bridge payload into the generic record_trade_live() shape.
    Best-effort: failures are swallowed so Sheets logging is never blocked.
    """
    try:
        rec  = j.get("receipt") or {}
        cmd  = j.get("command") or {}
        meta = j.get("meta")    or {}

        cmd_id = (
            j.get("cmd_id")
            or meta.get("cmd_id")
            or cmd.get("id")
            or j.get("id")
            or rcp_id
        )

        venue  = (rec.get("venue") or cmd.get("venue") or "").upper()
        symbol = rec.get("symbol") or cmd.get("symbol") or cmd.get("pair") or ""
        side   = (rec.get("side") or cmd.get("side") or "").upper()
        status = (rec.get("status") or j.get("status") or "").lower()

        # Quantities / price (best-effort)
        base_qty = rec.get("executed_qty") or rec.get("base_qty")
        quote_qty = (
            rec.get("amount_quote")
            or cmd.get("amount_quote")
            or cmd.get("amount_usd")
        )
        price = rec.get("avg_price") or rec.get("price")

        note  = rec.get("note") or rec.get("message") or rec.get("error") or ""
        did = (cmd.get("decision_id") or meta.get("decision_id") or rec.get("decision_id") or "")
        if did and ("decision_id=" not in str(note)):
            note = (str(note) + " " if note else "") + f"decision_id={did}"
        fills = rec.get("fills") or []

        req_sym = cmd.get("symbol") or cmd.get("pair") or ""
        res_sym = rec.get("resolved_symbol") or rec.get("symbol") or ""
        post_bal = rec.get("post_balances") or rec.get("post_balances_compact")

        payload = {
            "id": cmd_id,
            "agent_id": meta.get("agent_id") or cmd.get("agent_id") or j.get("agent_id"),
            "venue": venue,
            "symbol": symbol,
            "side": side,
            "status": status,
            "txid": rec.get("txid") or rec.get("order_id") or rec.get("trade_id") or "",
            "fills": fills,
            "note": note,
            "requested_symbol": req_sym,
            "resolved_symbol": res_sym,
            "post_balances": post_bal,
            "base_qty": base_qty,
            "quote_qty": quote_qty,
            "price": price,
        }

        record_trade_live(cmd_id, payload)
    except Exception:
        # Absolutely non-fatal; this is an observability mirror only.
        return

# ----------- main tick ----------

def run_once() -> Tuple[int, int]:
    """
    Returns (processed, written)
    - processed: # receipts examined
    - written  : # rows actually appended to sheet
    """
    last_id, last_run_epoch = _read_state()
    # quiet guard against accidental rapid looping; scheduler should control cadence
    if time.time() - last_run_epoch < max(0, MIN_INTERVAL_SEC // 2):
        # still allow if brand new instance, but stay quiet
        pass

    items = _fetch_new_receipts(last_id, MAX_PER_TICK)
    if not items:
        print("[bridge] no new receipts")
        _write_state(last_id)  # refresh heartbeat
        return (0, 0)

    to_write: List[List[Any]] = []
    new_last_id = last_id
    for it in items:
        new_last_id = max(new_last_id, int(it["id"]))
        try:
            j = json.loads(it["payload"])
        except Exception:
            # Bad JSON payload — skip but move state forward to avoid loop
            continue

        row = _build_row(it["id"], j)
        if row:
            to_write.append(row)
            # New: mirror trade into Postgres backbone (best-effort)
            _mirror_trade_to_db(it["id"], j)

    written = 0
    # batch quietly (single append if <= BATCH_SIZE)
    if to_write:
        # chunk into batches only if huge backlog
        for i in range(0, len(to_write), BATCH_SIZE):
            chunk = to_write[i:i+BATCH_SIZE]
            try:
                written += _append_rows(chunk)
            except Exception as e:
                print(f"[bridge] append error: {e}")
                traceback.print_exc()
                # Do not roll back state—avoid infinite rewrites. Just log.
    # update state no matter what to prevent re-backfilling spam
    _write_state(new_last_id)
    print(f"[bridge] processed={len(items)} wrote={written} last_id={new_last_id}")
    return (len(items), written)

# ----------- cli ----------

if __name__ == "__main__":
    once = "--once" in sys.argv or "-1" in sys.argv
    if once:
        run_once()
    else:
        # polite loop (fallback); production uses scheduler in wsgi.py
        interval = max(MIN_INTERVAL_SEC, 300)
        print(f"[bridge] running loop every {interval}s")
        while True:
            try:
                run_once()
            except Exception as e:
                print(f"[bridge] tick error: {e}")
                traceback.print_exc()
            time.sleep(interval)
