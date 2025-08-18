"""Timezone suggestion implementation."""

from typing import Optional
from pydantic import Field
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult
import os
import importlib
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.user_defined_strategy import UserDefinedStrategy

class TimezoneSuggestionAlgorithm(BaseAlgorithm):
    """Timezone suggestion algorithm using a pluggable strategy architecture"""

    def __init__(self, timezone=None):
        self.strategies = self._load_strategies()
        self.timezone = timezone

    def _load_strategies(self) -> list[TimezoneStrategy]:
        """Dynamically load all strategies from the 'strategies' directory."""
        strategies = []
        strategy_path = os.path.dirname(__file__) + '/strategies'
        for filename in os.listdir(strategy_path):
            if filename.endswith('.py') and filename not in ['base.py', '__init__.py', 'user_defined_strategy.py']:
                module_name = f"datnguyen.rule_auditor.suggestions.timezone.strategies.{filename[:-3]}"
                module = importlib.import_module(module_name)
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if isinstance(attribute, type) and issubclass(attribute, TimezoneStrategy) and attribute is not TimezoneStrategy:
                        strategies.append(attribute())
        return strategies

    @requires_metrics('count_weekday_distribution', 'count_date_label_lag_distribution', 'count_30_min_distribution')
    def suggest(self, statistics: StatisticsResult) -> Optional[TimezoneResult]:
        """Generate timezone suggestion using multiple strategies"""
        # User-defined strategy is handled separately and takes precedence
        user_strategy = UserDefinedStrategy(self.timezone)
        user_result = user_strategy.suggest(statistics)
        if user_result and user_result.timezone:
            return user_result

        priority_order = {'T': 1, 'B': 2, 'C': 3, 'b': 4, 'c': 5}
        combined_result = None

        # Sort strategies for deterministic execution if needed, e.g., by class name
        sorted_strategies = sorted(self.strategies, key=lambda s: s.__class__.__name__)

        for strategy in sorted_strategies:
            result = strategy.suggest(statistics)
            if result and result.timezone:
                if not combined_result or (
                        result.delay_code and combined_result.delay_code and
                        priority_order.get(result.delay_code, float('inf')) < priority_order.get(
                    combined_result.delay_code, float('inf'))
                ):
                    combined_result = result

        return combined_result
