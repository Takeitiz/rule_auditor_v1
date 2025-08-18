"""
Entropy-based timezone strategy.
"""

from typing import Dict, List, Optional
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.base import requires_metrics
from datnguyen.rule_auditor.suggestions.utils import calculate_entropy, calculate_time_by_percentile_from_distribution


class EntropyStrategy(TimezoneStrategy):
    """Suggest timezone based on entropy analysis"""

    @requires_metrics('count_mtime_weekday_distribution', 'count_mtime_date_label_lag_distribution',
                      'count_30_min_distribution')
    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[
        TimezoneResult]:
        # Get distributions
        weekday_dist = statistics.count_mtime_weekday_distribution
        lag_dist = statistics.count_mtime_date_label_lag_distribution

        # If specific timezone requested, only analyze that one
        if timezone:
            if timezone not in weekday_dist or timezone not in lag_dist:
                return TimezoneResult(timezone=None)
            result = TimezoneResult(timezone=timezone)
            result.method_used = "entropy"
            return result

        # Calculate entropy scores for each timezone
        entropy_scores = {}
        for tz in weekday_dist.keys():
            file_count_entropy = calculate_entropy(weekday_dist.get(tz, {}))
            date_label_lag_entropy = calculate_entropy(lag_dist.get(tz, {}))
            entropy_scores[tz] = file_count_entropy + date_label_lag_entropy

        if not entropy_scores:
            return TimezoneResult(timezone=None)

        # Find timezones with minimum entropy
        min_entropy = min(entropy_scores.values())
        best_timezones = [tz for tz, entropy in entropy_scores.items() if entropy == min_entropy]

        if len(best_timezones) == 1:
            result = TimezoneResult(timezone=best_timezones[0])
            result.method_used = "entropy"
            return result

        # If multiple timezones have the same entropy, select the one with the longest activity duration
        activity_durations = self._calculate_activity_durations(statistics.count_30_min_distribution, best_timezones)
        if activity_durations:
            best_tz = max(activity_durations.items(), key=lambda x: x[1])[0]
            result = TimezoneResult(timezone=best_tz)
            result.method_used = "entropy"
            return result

        return TimezoneResult(timezone=None)

    def _calculate_activity_durations(self, count_30_min_dist: Dict, timezones: List[str]) -> Dict[str, int]:
        """Calculate the total activity duration for each timezone using the 97th percentile."""
        durations = {}
        for tz in timezones:
            if tz in count_30_min_dist:
                duration = 86400 - calculate_time_by_percentile_from_distribution(count_30_min_dist[tz], percentiles=90)
                if duration is not None:
                    durations[tz] = duration
        return durations
