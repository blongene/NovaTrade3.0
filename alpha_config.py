"""
alpha_config.py

Centralized config loader to avoid env-var slot exhaustion.

Load order:
1) ALPHA_CONFIG_JSON (stringified JSON)
2) ALPHA_CONFIG_PATH (JSON file path; default /etc/secrets/alpha_config.json)
3) {} (empty)

Usage:
    from alpha_config import get_alpha_config, cfg_get
    cfg = get_alpha_config()
    val = cfg_get(cfg, "phase26.e.buy_usd_default", 10)
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict


_DEFAULT_PATH = "/etc/secrets/alpha_config.json"


def _try_parse_json(s: str) -> Dict[str, Any]:
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _try_read_json_file(path: str) -> Dict[str, Any]:
    try:
        if not path or not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


_cached: Dict[str, Any] | None = None


def get_alpha_config(force_reload: bool = False) -> Dict[str, Any]:
    global _cached
    if _cached is not None and not force_reload:
        return _cached

    s = os.getenv("ALPHA_CONFIG_JSON", "").strip()
    if s:
        _cached = _try_parse_json(s)
        return _cached

    path = os.getenv("ALPHA_CONFIG_PATH", _DEFAULT_PATH).strip() or _DEFAULT_PATH
    _cached = _try_read_json_file(path)
    return _cached


def cfg_get(cfg: Dict[str, Any], dotted_key: str, default: Any = None) -> Any:
    """
    Dotted lookup: cfg_get(cfg, "phase26.e.buy_usd_default", 10)
    """
    cur: Any = cfg
    for part in dotted_key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur
