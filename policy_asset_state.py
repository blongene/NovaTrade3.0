"""
policy_asset_state.py

Defines the AssetState structure used by the PolicyEngine.
This allows separation between policy logic and data sources.
"""

from typing import Dict, Any


class AssetState:
    """
    Container for venue budgets, memory signals, portfolio metrics, etc.

    Fields (all optional):
      - venue_budget: per-venue USD budgets
      - memory_score: token-specific memory pressure
      - portfolio_score: target vs actual weighting
      - vault_signal: vault intelligence signal
    """

    def __init__(
        self,
        venue_budget: Dict[str, float] | None = None,
        memory_score: Dict[str, float] | None = None,
        portfolio_score: Dict[str, float] | None = None,
        vault_signal: Dict[str, Any] | None = None,
    ):
        self.venue_budget = venue_budget or {}
        self.memory_score = memory_score or {}
        self.portfolio_score = portfolio_score or {}
        self.vault_signal = vault_signal or {}

    def get_venue_budget(self, venue: str) -> float | None:
        return self.venue_budget.get(venue)

    def get_memory_signal(self, token: str) -> float | None:
        return self.memory_score.get(token)

    def get_portfolio_score(self, token: str) -> float | None:
        return self.portfolio_score.get(token)

    def get_vault_signal(self, token: str) -> Any:
        return self.vault_signal.get(token)
