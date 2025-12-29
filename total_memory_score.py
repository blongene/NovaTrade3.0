# total_memory_score.py
"""
Compatibility shim.

Scheduler expects `total_memory_score`.
Actual implementation lives in total_memory_score_sync.sync_total_memory_score.
"""

import logging
logger = logging.getLogger(__name__)

def run_total_memory_score() -> None:
    try:
        from total_memory_score_sync import sync_total_memory_score
        sync_total_memory_score()
    except Exception as e:
        logger.exception("total_memory_score shim failed: %s", e)
