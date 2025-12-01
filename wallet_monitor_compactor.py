# wallet_monitor_compactor.py
"""
Standalone job: compacts Wallet_Monitor using the helper in telemetry_mirror.

Wiring:
- Scheduler calls run_wallet_monitor_compactor()
- Uses with_sheet_backoff so Sheets 429s are handled like the rest of Nova.
"""

from utils import with_sheet_backoff  # <-- brings in the decorator
from telemetry_mirror import _compact_wallet_monitor_if_needed


@with_sheet_backoff
def run_wallet_monitor_compactor() -> None:
    """Entry point for scheduled / manual compaction."""
    _compact_wallet_monitor_if_needed()


if __name__ == "__main__":
    # Handy for manual runs: python wallet_monitor_compactor.py
    run_wallet_monitor_compactor()
