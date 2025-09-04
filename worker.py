# worker.py — background worker that runs Nova boot/schedulers
import signal, sys, time

def _graceful_exit(signum, frame):
    print("[WORKER] SIGTERM received, shutting down…", flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, _graceful_exit)
signal.signal(signal.SIGINT, _graceful_exit)

from main import boot  # your existing orchestrated boot
boot()                  # starts schedules/threads and blocks in its loop
