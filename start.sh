#!/usr/bin/env bash
set -euo pipefail

# then run the web app (bind to Render's $PORT)
exec gunicorn wsgi:app -w 1 --threads 2 --bind 0.0.0.0:${PORT} \
  --timeout 120 --graceful-timeout 30 \
  --access-logfile - --error-logfile - --log-level info
