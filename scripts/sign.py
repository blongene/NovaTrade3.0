#!/usr/bin/env python3
import sys,json,hmac,hashlib
if len(sys.argv)!=3:
    print(f"usage: {sys.argv[0]} <secret> <json-body>",file=sys.stderr); exit(1)
secret,body=sys.argv[1],sys.argv[2]
canon=json.dumps(json.loads(body),separators=(',',':'),sort_keys=True).encode()
print(hmac.new(secret.encode(),canon,hashlib.sha256).hexdigest())
