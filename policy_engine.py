# policy_engine.py
import os, time, hmac, hashlib, json
from datetime import datetime, timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials

POLICY_FILE = os.getenv("POLICY_FILE", "policy.yaml")
POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")

def _load_yaml(path):
    # Minimal YAML subset loader; safe for the provided policy.yaml template.
    txt = ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read()
    except Exception:
        pass

    default = {
        "policy":{
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
    if not txt.strip():
        return default

    data = default
    cur = data["policy"]
    lines = [ln.rstrip() for ln in txt.splitlines() if ln.strip() and not ln.strip().startswith("#")]

    def _parse_list(v):
        v=v.strip()
        if v.startswith("[") and v.endswith("]"):
            inner = v[1:-1].strip()
            if not inner: return []
            return [x.strip() for x in inner.split(",")]
        return v

    # Super naive parser matching the provided template
    in_policy = False
    for ln in lines:
        if ln.startswith("policy:"):
            in_policy = True
            continue
        if not in_policy:
            continue
        if ":" in ln:
            k, v = ln.split(":", 1)
            k = k.strip()
            v = v.strip()
            if v.lower() in ("true","false"):
                cur[k] = (v.lower()=="true")
            elif v.replace(".","",1).isdigit():
                cur[k] = float(v) if "." in v else int(v)
            elif v.startswith("["):
                cur[k] = _parse_list(v)
            else:
                # nested maps (prefer_quotes) handled explicitly
                if k == "prefer_quotes":
                    cur[k] = {}
                elif k in ("BINANCEUS","COINBASE","KRAKEN"):
                    # assume belongs to prefer_quotes
                    if "prefer_quotes" not in cur: cur["prefer_quotes"] = {}
                    cur["prefer_quotes"][k] = v
                else:
                    cur[k] = v
    return data

def _open_sheet():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("sentiment-log-service.json", scope)
    client = gspread.authorize(creds)
    return client.open_by_url(os.getenv("SHEET_URL"))

def _append_policy_log(sh, row):
    try:
        try:
            ws = sh.worksheet(POLICY_LOG_WS)
        except Exception:
            ws = sh.add_worksheet(title=POLICY_LOG_WS, rows=1000, cols=12)
            ws.append_row(["Timestamp","Token","Action","Amount_USD","OK","Reason","Patched","Venue","Quote","Liquidity","Cooldown_Min"])
        ws.append_row(row)
    except Exception as e:
        print(f"⚠️ Policy log append failed: {e}")

class PolicyEngine:
    def __init__(self):
        self.cfg = _load_yaml(POLICY_FILE)["policy"]
        self.sh = _open_sheet()
        self.cooldown_min = int(self.cfg.get("cool_off_minutes_after_trade", 30))

    def _get_last_trade_ts(self, token:str):
        try:
            ws = self.sh.worksheet(POLICY_LOG_WS)
            rows = ws.get_all_records()
            latest = None
            for r in rows:
                if str(r.get("Token","")).strip().upper() == token.upper() and str(r.get("OK","")).upper() in ("TRUE","YES"):
                    ts = str(r.get("Timestamp","")).strip()
                    try:
                        t = datetime.fromisoformat(ts.replace("Z",""))
                    except:
                        continue
                    if (latest is None) or (t>latest):
                        latest = t
            return latest
        except:
            return None

    def validate(self, intent:dict, asset_state:dict):
        """
        intent: { token, action, amount_usd, venue, quote }
        asset_state: { token, liquidity_usd, memory_score, roi_7d, unlock_days, last_action_ts, ... }

        returns: (ok:bool, reason:str, patched_intent:dict)
        """
        token = intent.get("token","").upper()
        action = intent.get("action","").upper()
        amt    = float(intent.get("amount_usd", 0) or 0)
        venue  = intent.get("venue","")
        quote  = intent.get("quote","")

        cfg = self.cfg
        blocked = [s.upper() for s in cfg.get("blocked_symbols",[])]
        min_liq = float(cfg.get("min_liquidity_usd", 0) or 0)
        max_per = float(cfg.get("max_per_coin_usd", 0) or 0)

        if token in blocked:
            r="blocked symbol"
            _append_policy_log(self.sh, [datetime.utcnow().isoformat(), token, action, amt, "FALSE", r, "", venue, quote, asset_state.get("liquidity_usd",""), self.cooldown_min])
            return False, r, intent

        liq = asset_state.get("liquidity_usd")
        if liq not in ("", None):
            try:
                liq_f = float(liq)
                if liq_f < min_liq:
                    r="below liquidity threshold"
                    _append_policy_log(self.sh, [datetime.utcnow().isoformat(), token, action, amt, "FALSE", r, "", venue, quote, liq_f, self.cooldown_min])
                    return False, r, intent
            except:
                pass

        patched = dict(intent)
        if max_per and amt > max_per:
            patched["amount_usd"] = max_per

        last_ts = self._get_last_trade_ts(token)
        if last_ts:
            delta = datetime.utcnow() - last_ts
            if delta < timedelta(minutes=self.cooldown_min):
                r=f"cooldown active ({int((timedelta(minutes=self.cooldown_min)-delta).total_seconds()/60)} min left)"
                _append_policy_log(self.sh, [datetime.utcnow().isoformat(), token, action, amt, "FALSE", r, json.dumps(patched), venue, quote, asset_state.get("liquidity_usd",""), self.cooldown_min])
                return False, r, patched

        prefer = cfg.get("prefer_quotes",{}).get(venue, "")
        if prefer and quote != prefer:
            patched["quote"] = prefer

        _append_policy_log(self.sh, [datetime.utcnow().isoformat(), token, action, patched.get("amount_usd", amt), "TRUE", "ok", json.dumps(patched), venue, patched.get("quote", quote), asset_state.get("liquidity_usd",""), self.cooldown_min])
        return True, "ok", patched

# policy_utils.py (new) OR inside policy_engine.py
import os, time
from datetime import datetime
from utils import get_ws_cached, with_sheet_backoff

POLICY_LOG_WS = os.getenv("POLICY_LOG_WS", "Policy_Log")

HEADERS = ["Timestamp","Token","Action","Amount_USD","OK","Reason","Patched","Venue","Quote","Liquidity","Cooldown_Min"]

@with_sheet_backoff
def _ensure_header(ws):
    vals = ws.get_all_values()
    if not vals or (vals and vals[0] != HEADERS):
        ws.clear()
        ws.append_row(HEADERS)

def write_policy_log(decision: dict):
    """
    decision is expected to look like:
      {
        "ts": 1699999999, "token":"BTC", "action":"BUY", "amount_usd":5.0,
        "ok": True, "reason":"", "patched": "", "venue":"BINANCEUS",
        "quote":"USDT", "liquidity": 1_000_000, "cooldown_min": 0
      }
    """
    ws = get_ws_cached(POLICY_LOG_WS, ttl_s=30)
    _ensure_header(ws)
    ts = decision.get("ts") or int(time.time())
    row = [
      datetime.utcfromtimestamp(int(ts)).isoformat(timespec="seconds")+"Z",
      decision.get("token",""),
      decision.get("action",""),
      float(decision.get("amount_usd") or 0),
      "TRUE" if decision.get("ok") else "FALSE",
      decision.get("reason",""),
      decision.get("patched",""),
      decision.get("venue",""),
      decision.get("quote",""),
      float(decision.get("liquidity") or 0),
      int(decision.get("cooldown_min") or 0),
    ]
    ws.append_row(row)
