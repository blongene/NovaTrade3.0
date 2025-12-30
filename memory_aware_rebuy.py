"""
memory_aware_rebuy.py — compatibility wrapper (Phase 22B)

Your scheduler expects:
  memory_aware_rebuy.run_memory_aware_rebuy_engine()

Legacy module providing the logic is:
  rebuy_memory_engine.run_memory_rebuy_scan()

This wrapper preserves full behavior without changing rebuy_memory_engine.py.
"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)

def run_memory_aware_rebuy_engine():
    try:
        from rebuy_memory_engine import run_memory_rebuy_scan  # type: ignore
        return run_memory_rebuy_scan()
    except Exception as e:
        logger.warning("Memory-Aware Rebuy: rebuy_memory_engine.run_memory_rebuy_scan failed: %s", e)
        return None
