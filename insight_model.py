from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass
class CouncilInsight:
    """Canonical record for a single council decision.

    This is the schema that is written to `council_insights.jsonl` and later
    mirrored into Sheets / HTML views. It is intentionally forward-compatible:
    new optional fields may be added over time without breaking readers.
    """

    # Core identity / timing
    decision_id: str
    ts: float  # unix timestamp (seconds)

    # Governance context
    autonomy: str
    council: Dict[str, Any]

    # Human-readable story
    story: str
    ok: bool
    reason: str
    flags: List[str] = field(default_factory=list)

    # Intent payloads
    raw_intent: Dict[str, Any] = field(default_factory=dict)
    patched_intent: Dict[str, Any] = field(default_factory=dict)

    # Routing metadata
    venue: Optional[str] = None
    symbol: Optional[str] = None

    # Phase 21: Ash's Lens classification (e.g. "clean", "resized", "blocked")
    ash_lens: Optional[str] = None

    # Phase 21.2: execution + outcome metadata (populated later by analytics)
    exec_status: Optional[str] = None        # "done", "error", "pending", ...
    exec_notional_usd: Optional[float] = None
    exec_quote: Optional[str] = None         # e.g. "USDT", "USD"
    outcome_tag: Optional[str] = None        # "trade_success", "trade_error",
                                             # "policy_denied", "anomaly", ...

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a JSON-safe dict, omitting empty optionals.

        Older readers that expect only the original fields can simply ignore
        the newer keys; we keep everything flat and backwards-compatible.
        """
        data = asdict(self)
        # Drop Nones to keep the JSONL lean and stable for existing tooling.
        return {k: v for k, v in data.items() if v is not None}
