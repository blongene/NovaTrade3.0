
#!/usr/bin/env bash
# sign.sh — produce X-NT-Sig for NovaTrade Bus requests
secret="$1"; body="$2"
if [ -z "$secret" ] || [ -z "$body" ]; then
  echo "usage: $0 <secret> <json-body>" 1>&2; exit 1
fi
printf '%s' "$body" | openssl dgst -sha256 -hmac "$secret" | awk '{print $2}'
