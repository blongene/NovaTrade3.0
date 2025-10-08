
# ops_sign_and_enqueue.py
# Usage:
#   python ops_sign_and_enqueue.py --base https://your-app.onrender.com \
#     --secret f36e385d5b3c83e66209cdac0d815788e1459b49cc67b6a6159cfa4de34511b8 --agent edge-cb-1 \
#     --venue COINBASE --symbol BTC/USDT --side BUY --amount 25
#
# Requires: pip install requests

import argparse, json, time, hmac, hashlib, requests, sys

def now_ms() -> str:
    return str(int(time.time() * 1000))

def hmac_hex(secret: bytes, data: bytes) -> str:
    return hmac.new(secret, data, hashlib.sha256).hexdigest()

def attempt(base, secret, body_dict):
    url = base.rstrip('/') + '/ops/enqueue'
    raw = json.dumps(body_dict, separators=(',', ':'), sort_keys=True).encode()
    ts = now_ms()

    # Try a few common signing styles – one of these should match your hmac_auth
    trials = [
        ('body',     hmac_hex(secret, raw)),
        ('ts+body',  hmac_hex(secret, (ts + raw.decode()).encode())),
        ('body+ts',  hmac_hex(secret, (raw.decode() + ts).encode())),
        ('ts:body',  hmac_hex(secret, (ts + ':' + raw.decode()).encode())),
        ('ts.body',  hmac_hex(secret, (ts + '.' + raw.decode()).encode())),
        ('ts\\nbody',hmac_hex(secret, (ts + '\n' + raw.decode()).encode())),
    ]

    for label, sig in trials:
        headers = {
            'Content-Type': 'application/json',
            'X-Timestamp': ts,
            'X-Signature': sig
        }
        try:
            r = requests.post(url, data=raw, headers=headers, timeout=15)
            is_json = r.headers.get('content-type','').startswith('application/json')
            if r.status_code == 200 and is_json:
                j = r.json()
                if j.get('ok') is True:
                    cmd_id = j.get('id')
                    print(f"[SUCCESS via {label}] id={cmd_id}, status={r.status_code}")
                    return True, label, j
                else:
                    print(f"[{label}] HTTP 200 but ok!=True → {j}")
            else:
                snippet = r.text[:200] if r.text else ''
                print(f"[{label}] HTTP {r.status_code} → {snippet}")
        except Exception as err:
            print(f"[{label}] error: {err}")
    return False, None, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base', required=True, help='Base URL (e.g., https://novatrade3-0.onrender.com)')
    ap.add_argument('--secret', required=True, help='OUTBOX_SECRET')
    ap.add_argument('--agent', required=True, help='agent_id (e.g., edge-cb-1)')
    ap.add_argument('--venue', required=True, choices=['COINBASE','COINBASE_ADVANCED','BINANCE.US','MEXC','KRAKEN'])
    ap.add_argument('--symbol', required=True, help='e.g., BTC/USDT')
    ap.add_argument('--side', required=True, choices=['BUY','SELL'])
    ap.add_argument('--amount', required=True, help='numeric string (quote or base per executor config)')
    ap.add_argument('--tif', default='IOC')
    args = ap.parse_args()

    secret = args.secret.encode()
    body = {
        'agent_id': args.agent,
        'type': 'order.place',
        'payload': {
            'venue': args.venue,
            'symbol': args.symbol,
            'side': args.side,
            'amount': str(args.amount),
            'time_in_force': args.tif
        }
    }
    ok, label, j = attempt(args.base, secret, body)
    if not ok:
        print('All signing patterns failed. Check OUTBOX_SECRET, time sync, and that /ops/enqueue is deployed.')
        sys.exit(2)

if __name__ == '__main__':
    main()
