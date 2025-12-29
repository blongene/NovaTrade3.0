# undersized_rebuy.py
"""
Compatibility shim.

Some schedulers import undersized_rebuy by name.
Real implementation lives in rebuy_engine.run_undersized_rebuy.
"""

import logging
logger = logging.getLogger(__name__)

def run_undersized_rebuy() -> None:
    try:
        from rebuy_engine import run_undersized_rebuy as _impl
        _impl()
    except Exception as e:
        logger.exception("undersized_rebuy shim failed: %s", e)
