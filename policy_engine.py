# policy_engine.py — Phase-5 policy + logging + symbol mapping
import os, time, json
from datetime import datetime, timedelta

# Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials

POLICY_FILE    = os.getenv("POLICY_FILE", "policy.yaml")
POLICY_LOG_WS  = os.getenv("POLICY_LOG_WS", "Policy_Log")
SHEET_URL      = os.getenv("SHEET_URL")

MAJORS = {"BTC","ETH"}

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    return gspread.authorize(creds).open_by_url(SHEET_URL)

def _ensure_policy_log(ws):
    header = ["Timestamp","Token","Action","Amount_USD","OK","Reason","Patched","Venue","Quote","Liquidity","Cooldown_Min"]
    vals = ws.get_all_values()
    if not vals or vals[0] != header:
        ws.clear()
        ws.append_row(header)

def _append_policy_row(sh, row):
    try:
        try:
            ws = sh.worksheet(POLICY_LOG_WS)
        except Exception:
            ws = sh.add_worksheet(title=POLICY_LOG_WS, rows=1000, cols=12)
        _ensure_policy_log(ws)
        ws.append_row(row)
    except Exception as e:
        print(f"⚠️ Policy log append failed: {e}")

def _load_policy_yaml(path):
    # Minimal YAML subset loader aligned to your template
    default = {
        "policy": {
            "max_per_coin_usd": 25,
            "min_quote_reserve_usd": 10,
            "min_liquidity_usd": 50000,
            "rebuy_if_roi_drawdown_pct": 15,
            "cool_off_minutes_after_trade": 30,
            "prefer_quotes": {"BINANCEUS":"USDT","COINBASE":"USDC","KRAKEN":"USDT"},
            "venue_order": ["BINANCEUS","COINBASE","KRAKEN"],
            "blocked_symbols": ["BARK","BONK"]
        }
    }
    try:
        with open(path,"r",encoding="utf-8") as f:
            txt = f.read()
    except Exception:
        return default

    cur = dict(default["policy"])
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    in_policy = False
    for ln in lines:
        if ln.startswith("policy:"):
            in_policy = True
            continue
        if not in_policy or ":" not in ln:
            continue
        k, v = ln.split(":",1); k=k.strip(); v=v.strip()
        if k == "prefer_quotes": cur[k] = {}
        elif k in ("BINANCEUS","COINBASE","KRAKEN"):
            cur.setdefault("prefer_quotes", {})[k] = v
        elif v.lower() in ("true","false"): cur[k] = (v.lower()=="true")
        elif v.startswith("[") and v.endswith("]"):
            inner = v[1:-1].strip()
            cur[k] = [x.strip() for x in inner.split(",")] if inner else []
        elif v.replace(".","",1).isdigit():
            cur[k] = float(v) if "." in v else int(v)
        else:
            cur[k] = v
    return {"policy": cur}

def _symbol_for_venue(token:str, venue:str, quote:str):
    token = token.upper(); venue = (venue or "").upper(); quote = (quote or "").upper()
    if venue in ("COINBASE","COINBASEADV","CBADV"):
        return f"{token}-USD" if (quote in ("","USD","USDC")) else f"{token}-{quote}"
    if venue in ("BINANCEUS","BUSA"):
        return f"{token}{quote or 'USDT'}"
    if venue == "KRAKEN":
        # Kraken symbols vary; assume quote USDT and uppercase
        return f"{token}{quote or 'USDT'}"
    return token

class PolicyEngine:
    def __init__(self):
        self.cfg = _load_policy_yaml(POLICY_FILE)["policy"]
        self.sh  = _open_sheet()
        self.cooldown_min = int(self.cfg.get("cool_off_minutes_after_trade",30))

    def _last_ok_trade_ts(self, token:str):
        try:
            ws = self.sh.worksheet(POLICY_LOG_WS)
            rows = ws.get_all_records()
            latest = None
            for r in rows:
                if str(r.get("Token","")).upper()==token.upper() and str(r.get("OK","")).upper() in ("TRUE","YES"):
                    ts = str(r.get("Timestamp","")).replace("Z","")
                    try:
                        t = datetime.fromisoformat(ts)
                    except Exception:
                        continue
                    if latest is None or t>latest: latest = t
            return latest
        except Exception:
            return None

    def validate(self, intent:dict, asset_state:dict|None=None) -> dict:
        """
        Intent: {source, token, action, amount_usd, venue, quote, ts?}
        asset_state: optional metrics (liquidity_usd, roi_7d, unlock_days...)
        Returns a 'decision' dict with fields used for logging and enqueue.
        """
        asset_state = asset_state or {}
        cfg = self.cfg

        token = (intent.get("token") or "").upper()
        action = (intent.get("action") or "").upper()
        amt  = float(intent.get("amount_usd") or 0)
        venue = (intent.get("venue") or "").upper()
        quote = (intent.get("quote") or "").upper()
        ts    = int(intent.get("ts") or time.time())

        # prefer quote
        prefer = cfg.get("prefer_quotes",{}).get(venue,"")
        if prefer and quote != prefer:
            quote = prefer

        # map symbol
        symbol = intent.get("symbol") or _symbol_for_venue(token, venue, quote)

        # blocked
        if token in [s.upper() for s in cfg.get("blocked_symbols",[])]:
            return self._log_and_build(ts, token, action, amt, False, "blocked symbol", venue, quote, asset_state, patched={"symbol":symbol})

        # liquidity floor (skip for manual majors)
        liq = float(asset_state.get("liquidity_usd") or 0)
        min_liq = float(cfg.get("min_liquidity_usd") or 0)
        if not (intent.get("source")=="manual_rebuy" and token in MAJORS):
            if liq and liq < min_liq:
                return self._log_and_build(ts, token, action, amt, False, "below liquidity threshold", venue, quote, asset_state, patched={"symbol":symbol})

        # cap notional
        max_per = float(cfg.get("max_per_coin_usd") or 0)
        patched_amt = min(amt, max_per) if max_per else amt

        # cooldown
        last = self._last_ok_trade_ts(token)
        if last:
            delta = datetime.utcnow() - last
            if delta < timedelta(minutes=self.cooldown_min):
                left_min = int((timedelta(minutes=self.cooldown_min)-delta).total_seconds()/60)
                return self._log_and_build(ts, token, action, amt, False, f"cooldown active ({left_min} min left)", venue, quote, asset_state,
                                           patched={"amount_usd":patched_amt,"symbol":symbol})

        # OK
        return self._log_and_build(ts, token, action, patched_amt, True, "ok", venue, quote, asset_state,
                                   patched={"amount_usd":patched_amt,"symbol":symbol})

    # — helpers —
    def _log_and_build(self, ts, token, action, amount_usd, ok, reason, venue, quote, asset_state, patched=None):
        patched = patched or {}
        row = [
            datetime.utcfromtimestamp(int(ts)).isoformat(timespec="seconds")+"Z",
            token, action, float(amount_usd),
            "TRUE" if ok else "FALSE",
            reason,
            json.dumps(patched) if patched else "",
            venue, quote,
            float(asset_state.get("liquidity_usd") or 0),
            int(self.cooldown_min)
        ]
        _append_policy_row(self.sh, row)
        return {
            "ts": ts, "token": token, "action": action,
            "amount_usd": float(amount_usd),
            "ok": bool(ok), "reason": reason,
            "patched": patched, "venue": venue, "quote": quote,
            "liquidity": float(asset_state.get("liquidity_usd") or 0),
            "cooldown_min": int(self.cooldown_min),
            "symbol": patched.get("symbol") or None
        }
