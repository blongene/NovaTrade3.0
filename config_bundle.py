"""config_bundle.py — Render env-var packer/unpacker (Phase 29 parallel hardening)

Why this exists
--------------
Render has a limit on the number of environment variables. NovaTrade already uses
several packed JSON env vars (e.g., DB_READ_JSON, ALPHA_CONFIG_JSON,
PHASE26_DELTA_JSON). This module enables an *optional* single bundle variable:

  CONFIG_BUNDLE_JSON={
    "version": "2026-01-17",
    "vars": {
      "DB_READ_JSON": { ... },
      "ALPHA_CONFIG_JSON": { ... },
      "PHASE26_DELTA_JSON": { ... },
      "PHASE22A_JSON": { ... }
    }
  }

Behavior
--------
- If CONFIG_BUNDLE_JSON is not set: no-op.
- If set: for each entry in bundle["vars"], we set os.environ[KEY] to
  json.dumps(value) *only if* that env var is not already set.
- Never raises: returns a list of warnings instead.

This is intentionally conservative for Phase 29: it reduces config drift without
introducing new runtime behavior.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple


def _try_parse_json(raw: str) -> Tuple[Dict[str, Any], str | None]:
    raw = (raw or "").strip()
    if not raw:
        return {}, None
    # tolerate accidental backticks / surrounding quotes
    if (raw.startswith("`") and raw.endswith("`")) or (raw.startswith("\"") and raw.endswith("\"")):
        raw = raw.strip("`\"")
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return {}, "CONFIG_BUNDLE_JSON is not an object"
        return obj, None
    except Exception as e:
        return {}, f"CONFIG_BUNDLE_JSON invalid JSON: {e}"


def apply_config_bundle() -> List[str]:
    """Apply CONFIG_BUNDLE_JSON into individual env vars.

    Returns a list of warnings (empty list == clean).
    """
    warnings: List[str] = []

    bundle_raw = os.getenv("CONFIG_BUNDLE_JSON", "")
    if not (bundle_raw or "").strip():
        return warnings

    bundle, err = _try_parse_json(bundle_raw)
    if err:
        return [err]

    # support both {"vars": {...}} and flat {"DB_READ_JSON": {...}}
    vars_obj: Dict[str, Any]
    if "vars" in bundle and isinstance(bundle.get("vars"), dict):
        vars_obj = bundle.get("vars")  # type: ignore
    else:
        vars_obj = {k: v for k, v in bundle.items() if k not in ("version", "updated_at", "meta")}

    if not vars_obj:
        warnings.append("CONFIG_BUNDLE_JSON has no 'vars' entries")
        return warnings

    for k, v in vars_obj.items():
        if not isinstance(k, str) or not k:
            continue
        # only set if missing to preserve overrides
        if (os.getenv(k) or "").strip():
            continue
        try:
            os.environ[k] = json.dumps(v)
        except Exception as e:
            warnings.append(f"CONFIG_BUNDLE_JSON failed to materialize {k}: {e}")

    # optional hygiene warnings
    if os.getenv("EDGE_SECRET") and os.getenv("TELEMETRY_SECRET") and os.getenv("EDGE_SECRET") == os.getenv("TELEMETRY_SECRET"):
        warnings.append("EDGE_SECRET == TELEMETRY_SECRET (consider splitting secrets)")

    return warnings


def bundle_version() -> str:
    raw = os.getenv("CONFIG_BUNDLE_JSON", "")
    b, err = _try_parse_json(raw)
    if err or not b:
        return ""
    v = b.get("version") or (b.get("meta") or {}).get("version")
    return str(v) if v else ""
