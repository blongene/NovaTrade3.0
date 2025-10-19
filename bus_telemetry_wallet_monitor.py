# bus_telemetry_wallet_monitor.py
import os
import json
from datetime import datetime
from flask import Blueprint, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials

bp = Blueprint("telemetry_wallet_monitor", __name__)

SHEET_URL = os.getenv("SHEET_URL")
WALLET_MONITOR_WS = os.getenv("WALLET_MONITOR_WS", "Wallet_Monitor")

def _open_sheet():
    scope=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    svc_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/etc/secrets/sentiment-log-service.json")
    creds = ServiceAccountCredentials.from_json_keyfile_name(svc_path, scope)
    return gspread.authorize(creds).open_by_url(SHEET_URL)

def _safe_num(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def _upsert_wallet_monitor(balances: dict):
    sh = _open_sheet()
    try:
        try:
            ws = sh.worksheet(WALLET_MONITOR_WS); ws.clear()
        except Exception:
            ws = sh.add_worksheet(title=WALLET_MONITOR_WS, rows=2000, cols=10)

        headers = ["Timestamp","Venue","Asset","Free","Locked","Quote"]
        ws.append_row(headers)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for venue, assets in (balances or {}).items():
            for asset, amt in (assets or {}).items():
                asset_u = str(asset).upper()
                quote = asset_u if asset_u in ("USDT","USDC","USD","EUR") else ""
                rows.append([now, str(venue).upper(), asset_u, _safe_num(amt), 0.0, quote])

        if rows:
            ws.append_rows(rows)
        return len(rows)
    except Exception as e:
        raise

@bp.route("/api/telemetry/push_balances", methods=["POST"])
def telemetry_push_balances():
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception as e:
        return jsonify({"ok": False, "err": f"bad json: {e}"}), 400

    balances = (payload or {}).get("balances", {})
    if not isinstance(balances, dict) or not balances:
        return jsonify({"ok": False, "err": "missing or empty balances"}), 400

    try:
        n = _upsert_wallet_monitor(balances)
        return jsonify({"ok": True, "rows": n})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)}), 500
