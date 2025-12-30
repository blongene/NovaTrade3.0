"""
rotation_memory_weighted.py — compatibility shim

Your scheduler/main occasionally imports rotation_memory_weighted.
Some repos only ship rotation_memory.py (run_rotation_memory).

This shim keeps the system operational and removes "Import skipped" warnings.
"""
from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

def run_rotation_memory_weighted():
    try:
        from rotation_memory import run_rotation_memory  # type: ignore
        return run_rotation_memory()
    except Exception as e:
        logger.warning("rotation_memory_weighted: unable to run rotation_memory fallback: %s", e)
        return None
