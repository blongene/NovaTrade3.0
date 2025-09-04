# Procfile
web: bash -lc 'exec gunicorn wsgi:app -w 1 --threads 2 --bind 0.0.0.0:$PORT --timeout 60 --graceful-timeout 20 --access-logfile - --error-logfile - --log-level info'
worker: bash -lc 'python worker.py'
