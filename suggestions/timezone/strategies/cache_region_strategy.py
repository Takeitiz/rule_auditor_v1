"""
Cache region-based timezone strategy.
"""

from typing import Optional
from datnguyen.rule_auditor.const import REGION_TIMEZONE_MAP
from datnguyen.rule_auditor.suggestions.base import requires_metrics
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy


class CacheRegionStrategy(TimezoneStrategy):
    """Suggest timezone based on pattern region mapping"""

    @requires_metrics('pattern_region_counts')
    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[
        TimezoneResult]:
        region_counts = statistics.pattern_region_counts
        if not region_counts:
            return TimezoneResult(timezone=None)

        # If specific timezone requested, check if it matches any region
        if timezone:
            matching_regions = [r for r, tz in REGION_TIMEZONE_MAP.items() if tz == timezone]
            if not matching_regions:
                return TimezoneResult(timezone=None)
            result = TimezoneResult(timezone=timezone)
            result.method_used = "cache_region"
            return result

        # Calculate timezone scores based on region counts
        timezone_scores = {}
        total_count = sum(region_counts.values())

        for region, count in region_counts.items():
            if region == 'GLOBAL':
                # For GLOBAL, check if it's dominant
                if count / total_count > 0.7:
                    timezone_scores['GMT'] = timezone_scores.get('GMT', 0) + count
                else:
                    # Otherwise increment all major timezones
                    timezone_scores['America/New_York'] = timezone_scores.get('America/New_York', 0) + count
                    timezone_scores['Europe/London'] = timezone_scores.get('Europe/London', 0) + count
                    timezone_scores['Asia/Tokyo'] = timezone_scores.get('Asia/Tokyo', 0) + count
            else:
                timezone = REGION_TIMEZONE_MAP.get(region)
                if timezone:
                    timezone_scores[timezone] = timezone_scores.get(timezone, 0) + count

        if not timezone_scores:
            return TimezoneResult(timezone=None)

        # Get timezone with highest score
        best_tz = max(timezone_scores.items(), key=lambda x: x[1])[0]
        result = TimezoneResult(timezone=best_tz)
        result.method_used = "cache_region"
        return result
