# worker.py — background worker that runs Nova boot/schedulers

import os, signal, sys, time

def _graceful_exit(signum, frame):
    print("[WORKER] SIGTERM received, shutting down…", flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, _graceful_exit)
signal.signal(signal.SIGINT, _graceful_exit)

# This process should NOT set Telegram webhook (web/gunicorn will).
os.environ.setdefault("SET_WEBHOOK_IN_THIS_PROCESS", "0")

from main import boot
boot()  # starts schedules/threads

# Keep the worker process alive so daemon threads don’t die
while True:
    time.sleep(60)
