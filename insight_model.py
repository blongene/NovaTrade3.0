# insight_model.py

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import time
import uuid

@dataclass
class CouncilInsight:
    decision_id: str
    ts: float
    autonomy: str
    council: Dict[str, float]
    story: str
    ok: bool
    reason: str
    flags: list
    raw_intent: Dict[str, Any]
    patched_intent: Dict[str, Any]
    venue: Optional[str] = None
    symbol: Optional[str] = None

    def to_dict(self):
        return {
            "decision_id": self.decision_id,
            "ts": self.ts,
            "autonomy": self.autonomy,
            "council": self.council,
            "story": self.story,
            "ok": self.ok,
            "reason": self.reason,
            "flags": self.flags,
            "raw_intent": self.raw_intent,
            "patched_intent": self.patched_intent,
            "venue": self.venue,
            "symbol": self.symbol,
        }
