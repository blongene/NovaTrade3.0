"""
rotation_memory_weighted.py — compatibility shim (Phase 22B)

Scheduler expects:
  rotation_memory_weighted.run_rotation_memory_weighted()

Legacy module:
  rotation_memory.run_rotation_memory()
"""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)

def run_rotation_memory_weighted():
    try:
        from rotation_memory import run_rotation_memory  # type: ignore
        return run_rotation_memory()
    except Exception as e:
        logger.warning("rotation_memory_weighted: fallback failed: %s", e)
        return None
