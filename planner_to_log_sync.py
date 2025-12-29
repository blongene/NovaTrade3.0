# planner_to_log_sync.py
"""
Compatibility shim.

Some schedulers import planner_to_log_sync by name.
Real implementation lives in rotation_executor.sync_confirmed_to_rotation_log.
"""

import logging
logger = logging.getLogger(__name__)

def run_planner_to_log_sync() -> None:
    try:
        from rotation_executor import sync_confirmed_to_rotation_log
        sync_confirmed_to_rotation_log()
    except Exception as e:
        logger.exception("planner_to_log_sync shim failed: %s", e)
