"""
DST impact-based timezone strategy.
"""

from typing import Optional
from datnguyen.rule_auditor.suggestions.base import requires_metrics
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy

class DSTStrategy(TimezoneStrategy):
    """Suggest timezone based on DST impact analysis"""

    @requires_metrics('count_30_min_dst_distribution', 'count_30_min_non_dst_distribution')
    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        # If specific timezone requested, only check that one
        if timezone:
            if self._check_dst_impact(timezone, statistics):
                result = TimezoneResult(timezone='GMT')
                result.method_used = "dst_analysis"
                return result
            return TimezoneResult(timezone=None)

        # Check DST impact for major timezones
        for tz in ['America/New_York', 'Europe/London']:
            if self._check_dst_impact(tz, statistics):
                result = TimezoneResult(timezone='GMT')
                result.method_used = "dst_analysis"
                return result

        return TimezoneResult(timezone=None)

    def _check_dst_impact(self, timezone: str, statistics: StatisticsResult) -> bool:
        """Check if DST transitions impact file availability times"""
        dst_dist = statistics.count_30_min_dst_distribution.get(timezone, {})
        non_dst_dist = statistics.count_30_min_non_dst_distribution.get(timezone, {})

        if not dst_dist or not non_dst_dist:
            return False

        # Find peak times
        dst_peak = max(dst_dist.items(), key=lambda x: x[1])[0]
        non_dst_peak = max(non_dst_dist.items(), key=lambda x: x[1])[0]

        # Convert to minutes
        dst_minutes = int(dst_peak[:2]) * 60 + (30 if dst_peak[2:] == '30' else 0)
        non_dst_minutes = int(non_dst_peak[:2]) * 60 + (30 if non_dst_peak[2:] == '30' else 0)

        # Check for ~1 hour difference
        time_diff = abs(dst_minutes - non_dst_minutes)
        return 45 <= time_diff <= 75  # Allow for some variance around 1 hour
