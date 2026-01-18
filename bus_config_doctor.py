"""bus_config_doctor.py — boot-time config validation (warnings only).

Phase 29 safe: no behavior changes, no network calls.
Emits ONE concise line so operators can catch JSON/env drift early.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


def _env(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()


@dataclass
class DoctorResult:
    ok: bool
    warnings: List[str]
    hints: List[str]


def _try_json(label: str, raw: str) -> Tuple[bool, Any]:
    if not raw:
        return True, None
    try:
        return True, json.loads(raw)
    except Exception:
        # Some values are copy/paste tolerant (single quotes, trailing commas)
        # We intentionally do NOT attempt unsafe repairs here.
        return False, None


def diagnose() -> DoctorResult:
    warnings: List[str] = []
    hints: List[str] = []

    # 1) Validate known packed JSON env vars
    json_keys = [
        "DB_READ_JSON",
        "PHASE22A_JSON",
        "PHASE25_CANON_JSON",
        "PHASE26_DELTA_JSON",
        "PHASE28_JSON",
        "ALPHA_JSON",
    ]
    for k in json_keys:
        raw = _env(k)
        ok, _ = _try_json(k, raw)
        if not ok:
            warnings.append(f"{k} is not valid JSON")
            hints.append(f"Re-save {k} as strict JSON (double quotes, no trailing commas)")

    # 2) Lightweight doctrine-related warnings (do not block)
    db_raw = _env("DB_READ_JSON")
    ok, cfg = _try_json("DB_READ_JSON", db_raw)
    if ok and isinstance(cfg, dict):
        # Auto-heal is powerful; warn if enabled during observation.
        parity = cfg.get("parity") or {}
        if isinstance(parity, dict):
            if str(parity.get("auto_heal", "0")).lower() in {"1", "true", "yes"}:
                warnings.append("DB_READ_JSON.parity.auto_heal enabled (mutation risk)")
                hints.append("Prefer auto_heal=0 during observation; use manual or gated healing")

        # Enforcement (if present) should remain off in Phase 29.
        enforcement = cfg.get("enforcement") or {}
        if isinstance(enforcement, dict):
            if str(enforcement.get("enabled", "0")).lower() in {"1", "true", "yes"}:
                warnings.append("DB_READ_JSON.enforcement.enabled=1 (higher surprise surface)")

    ok_all = len(warnings) == 0
    return DoctorResult(ok=ok_all, warnings=warnings, hints=hints)


def emit_once(prefix: str = "BUS_CONFIG") -> DoctorResult:
    try:
        r = diagnose()
        if r.ok:
            print(f"[{prefix}] PASS")
        else:
            joined = " | ".join(r.warnings[:6])
            more = "" if len(r.warnings) <= 6 else f" (+{len(r.warnings)-6} more)"
            print(f"[{prefix}] WARN {len(r.warnings)} — {joined}{more}")
        return r
    except Exception:
        print(f"[{prefix}] WARN 1 — config_doctor_failed")
        return DoctorResult(ok=False, warnings=["config_doctor_failed"], hints=[])
