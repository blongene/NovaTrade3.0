#!/usr/bin/env python3
"""
policy_decision.py

Canonical representation of a policy decision emitted by the policy engine
(and, later, by other policy surfaces such as trade_guard).

This is intentionally lightweight and backwards-compatible:

  * Callers that expect a plain dict still receive a dict.
  * Existing fields (ok, status, reason, intent, patched) are preserved.
  * New metadata fields (limits_applied, council_trace, etc.) are optional
    and only appear when populated.

Phase 20:
  - Adds support for "limits_applied" and "council_trace", allowing the
    Council (Nova, Orion, Ash, Lumen, Vigil, Astraeus) to leave a trail on
    each decision without breaking legacy flows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid


@dataclass
class PolicyDecision:
    # Core decision shape
    ok: bool
    status: str
    reason: str
    intent: Dict[str, Any]
    patched: Dict[str, Any]

    # Metadata
    decision_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # Simple meta fields (already used in some flows)
    source: str = ""
    venue: str = ""
    symbol: str = ""
    base: str = ""
    quote: str = ""
    requested_amount_usd: Optional[float] = None
    approved_amount_usd: Optional[float] = None

    # Phase 20 extensions
    # - which policy limits were applied ("canary_cap", "prefer_quote", etc.)
    limits_applied: List[str] = field(default_factory=list)

    # - which Council voices influenced this decision
    #   e.g. {"astraeus": {"role":"path","stage":"post_engine"}, ...}
    council_trace: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Backwards-compatible dict representation that callers of trade_guard
        and the policy engine can consume.

        Top-level keys:
          - ok, status, reason, intent, patched, decision_id, created_at
        Meta is grouped under "meta" and only includes non-empty values.
        """
        base = {
            "ok": self.ok,
            "status": self.status,
            "reason": self.reason,
            "intent": self.intent,
            "patched": self.patched,
            "decision_id": self.decision_id,
            "created_at": self.created_at,
        }

        meta = {
            "source": self.source,
            "venue": self.venue,
            "symbol": self.symbol,
            "base": self.base,
            "quote": self.quote,
            "requested_amount_usd": self.requested_amount_usd,
            "approved_amount_usd": self.approved_amount_usd,
            "limits_applied": self.limits_applied,
            "council_trace": self.council_trace,
        }

        # Only include non-empty meta entries; allow 0.0 as valid
        meta_clean = {
            k: v
            for k, v in meta.items()
            if v is not None and v != ""  # lists/dicts are kept
        }
        if meta_clean:
            base["meta"] = meta_clean

        return base
