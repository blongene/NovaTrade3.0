"""
policy_bias.py

Loads bias controls from the Policy_Bias sheet.
These biases apply tilts (+/-) to USD amounts before clipping.
"""

from typing import Dict, Any, Optional
import math


class PolicyBias:
    def __init__(self, rows: list[dict]):
        """
        Expects rows from the Policy_Bias sheet (CSV or Sheets).
        Columns typically:
          Token | Bias_USD | Bias_Pct | Notes
        """
        self.bias_map: Dict[str, Dict[str, float]] = {}
        for r in rows:
            token = str(r.get("Token") or "").upper()
            if not token:
                continue

            entry = {
                "usd": float(r.get("Bias_USD") or 0),
                "pct": float(r.get("Bias_Pct") or 0),
            }
            self.bias_map[token] = entry

    def apply(self, token: str, amount_usd: float) -> float:
        """
        Apply USD and % tilt adjustments.
        Returns adjusted amount.
        """
        token_up = token.upper()
        data = self.bias_map.get(token_up)
        if not data:
            return amount_usd

        bias_usd = data.get("usd", 0)
        bias_pct = data.get("pct", 0)

        amt = amount_usd + bias_usd
        if bias_pct:
            amt = amt * (1 + bias_pct / 100.0)

        # never negative
        return max(amt, 0.0)

    def info(self, token: str) -> Optional[Dict[str, float]]:
        return self.bias_map.get(token.upper())
