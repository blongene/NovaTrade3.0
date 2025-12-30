"""
undersized_rebuy.py — compatibility wrapper (Phase 22B)

Your scheduler expects:
  undersized_rebuy.run_undersized_rebuy_engine()

Legacy module providing the logic is:
  rebuy_engine.run_undersized_rebuy()

This wrapper preserves full behavior without changing rebuy_engine.py.
"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)

def run_undersized_rebuy_engine():
    try:
        from rebuy_engine import run_undersized_rebuy  # type: ignore
        return run_undersized_rebuy()
    except Exception as e:
        logger.warning("Undersized Rebuy: rebuy_engine.run_undersized_rebuy failed: %s", e)
        return None
