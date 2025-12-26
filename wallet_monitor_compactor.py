# wallet_monitor_compactor.py — Bus
"""
Standalone job: compacts Wallet_Monitor using telemetry_mirror helper.

Wiring:
- Scheduler calls run_wallet_monitor_compactor()
- Uses with_sheet_backoff so Sheets 429s are handled consistently.
"""

from utils import with_sheet_backoff


@with_sheet_backoff
def run_wallet_monitor_compactor() -> None:
    """Entry point for scheduled / manual compaction."""
    try:
        from telemetry_mirror import _compact_wallet_monitor_if_needed  # type: ignore
    except Exception as e:
        print(f"⚠️ wallet_monitor_compactor: telemetry_mirror helper import failed: {e}")
        return

    try:
        _compact_wallet_monitor_if_needed()
    except Exception as e:
        print(f"⚠️ wallet_monitor_compactor: compaction failed: {e}")


if __name__ == "__main__":
    run_wallet_monitor_compactor()
