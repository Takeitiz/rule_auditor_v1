"""
User-defined timezone strategy.
"""

from typing import Optional
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult


class UserDefinedStrategy(TimezoneStrategy):
    def __init__(self, timezone: str = None):
        self.timezone = timezone

    def suggest_timezone(self, statistics, timezone=None) -> Optional[TimezoneResult]:
        return TimezoneResult(timezone=self.timezone)
