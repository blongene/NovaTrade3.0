# src/logging_setup.py
import logging
import os

# Default to WARNING globally unless explicitly overridden by env.
root_level = os.getenv("LOG_LEVEL", "WARNING").upper()

# Configure the root logger once.
logging.basicConfig(
    level=getattr(logging, root_level, logging.WARNING),
    format="%(levelname)s %(name)s: %(message)s",
)

# Silence chatty third-party loggers unless you need them.
for noisy, level in {
    "gspread": "WARNING",
    "googleapiclient": "WARNING",
    "google": "WARNING",
    "urllib3": "ERROR",
    "httpx": "WARNING",
    "asyncio": "WARNING",
    "apscheduler": "WARNING",
}.items():
    logging.getLogger(noisy).setLevel(getattr(logging, level, logging.WARNING))

# Optional: silence your own “boot thread”/“receipts_bridge” module unless it’s an error.
# Change 'nova' and 'receipts_bridge' to your actual module names if different.
for app_mod in ("nova", "receipts_bridge"):
    logging.getLogger(app_mod).setLevel(logging.WARNING)
