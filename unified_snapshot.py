import os, time, json
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SHEET_URL = os.getenv("SHEET_URL")
SNAP_WS = os.getenv("UNIFIED_SNAPSHOT_WS", "Unified_Snapshot")

def _open():
    scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds=ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(SHEET_URL)

def _safe_num(x):
    try: return float(str(x).replace(",","").strip())
    except: return 0.0

def run_unified_snapshot():
    """
    Gathers balances from any available tabs your stack already writes:
      - Wallet_Monitor (if present)  columns: Venue, Asset, Free, Locked, Quote
      - Edge/Receipts/Telemetry mirrors (optional/future)
    Writes a normalized table to Unified_Snapshot:
      [Timestamp, Venue, Asset, Free, Locked, Total, IsQuote, QuoteSymbol, Equity_USD]
    Degrades gracefully if a tab is missing.
    """
    print("📸 Building Unified Snapshot …")
    sh = _open()

    # Try Wallet_Monitor first
    src = []
    try:
        wm = sh.worksheet("Wallet_Monitor").get_all_records()
        for r in wm:
            venue = str(r.get("Venue","")).strip().upper()
            asset = str(r.get("Asset","")).strip().upper()
            free  = _safe_num(r.get("Free", 0))
            locked= _safe_num(r.get("Locked",0))
            quote = str(r.get("Quote","")).strip().upper()  # e.g., USDT/USDC
            total = free + locked
            src.append((venue, asset, free, locked, total, quote))
    except Exception:
        pass

    # Fallback: allow a very small manual source if Wallet_Monitor absent
    # (You can remove this once Wallet_Monitor is guaranteed present.)
    if not src:
        try:
            wb = sh.worksheet("Balances").get_all_records()
            for r in wb:
                venue = str(r.get("Venue","")).strip().upper()
                asset = str(r.get("Asset","")).strip().upper()
                total = _safe_num(r.get("Total",0))
                src.append((venue, asset, total, 0.0, total, ""))  # no locking/quote info
        except Exception:
            pass

    # Write snapshot
    try:
        try:
            ws = sh.worksheet(SNAP_WS); ws.clear()
        except Exception:
            ws = sh.add_worksheet(title=SNAP_WS, rows=2000, cols=10)

        headers = ["Timestamp","Venue","Asset","Free","Locked","Total","IsQuote","QuoteSymbol","Equity_USD"]
        ws.append_row(headers)

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for venue, asset, free, locked, total, quote in src:
            is_quote = "TRUE" if asset in ("USDT","USDC","USD","EUR") else "FALSE"
            rows.append([now, venue, asset, free, locked, total, is_quote, quote or "", ""])  # Equity_USD left blank for now

        if rows:
            ws.append_rows(rows)
            print(f"✅ Unified_Snapshot updated with {len(rows)} rows")
        else:
            print("ℹ️ No balances found; snapshot empty (ok).")
    except Exception as e:
        print(f"⚠️ Unified_Snapshot write error: {e}")
