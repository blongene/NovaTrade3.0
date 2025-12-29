# memory_aware_rebuy.py
"""
Compatibility shim.

Some schedulers import memory_aware_rebuy by name.
Real implementation lives in rebuy_memory_engine.run_memory_rebuy_scan.
"""

import logging
logger = logging.getLogger(__name__)

def run_memory_aware_rebuy() -> None:
    try:
        from rebuy_memory_engine import run_memory_rebuy_scan as _impl
        _impl()
    except Exception as e:
        logger.exception("memory_aware_rebuy shim failed: %s", e)
