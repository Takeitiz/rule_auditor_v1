"""
Check windows suggestion implementation.
"""
from typing import Optional
from datnguyen.rule_auditor.const import TIMEZONE_MAP_REVERSE
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.suggestions.check_windows.models import CheckWindowsResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.utils import calculate_time_by_percentile_from_distribution

class CheckWindowsSuggestionAlgorithm(BaseAlgorithm):
    """Check windows suggestion algorithm for a specific timezone"""

    def __init__(self, timezone: str):
        self.timezone = timezone

    @requires_metrics('count_weekday_distribution', 'count_30_min_distribution', 'holiday_metrics')
    def suggest(self, statistics: StatisticsResult) -> Optional[CheckWindowsResult]:
        """Generate check windows suggestion based on statistics for the specified timezone"""
        result = CheckWindowsResult(timezone=self.timezone)
        result.method_used = "combined_analysis"
        result.weekdays = suggest_weekday(statistics, self.timezone)
        time_dist = statistics.count_30_min_distribution.get(self.timezone, {})
        start_time = calculate_time_by_percentile_from_distribution(time_dist, 90)
        result.start_time = start_time
        result.end_time = 86340  # 23:59
        holiday = suggest_holiday(statistics)
        result.holiday_calendar = holiday

        return result


def suggest_weekday(statistics: StatisticsResult, short_tz):
    weekday_dist = statistics.count_mtime_weekday_distribution.get(short_tz, {})
    if weekday_dist:
        mean = sum(weekday_dist.values()) / len(weekday_dist) / 2
        active_days = [day for day, count in weekday_dist.items() if count > mean]
        if active_days:
            return ",".join(map(str, sorted(active_days)))


def suggest_holiday(statistics: StatisticsResult):
    """Generate holiday suggestion based on statistics"""
    holiday_similarity = statistics.holiday_metrics
    if not holiday_similarity:
        return None

    # Get top1 for each timezone
    timezone_top1 = {}
    for tz, country_data in holiday_similarity.items():
        sorted_matches = sorted([(k, v) for k, v in country_data.items()],
                                key=lambda x: x[1].get('similarity_score', 0), reverse=True)
        if sorted_matches:  # Add check for empty list
            timezone_top1[tz] = sorted_matches[0][1]

    if not timezone_top1:  # Add check for empty dictionary
        return None

    # Extract all top1 countries and scores
    all_top1_countries = [data.get('country', '') for data in timezone_top1.values()]
    all_top1_scores = [data.get('similarity_score', 0) for data in timezone_top1.values()]

    # Find most common country and its best shift
    most_common_country = max(set(all_top1_countries), key=all_top1_countries.count)

    # Rule 1: All timezones show the same pattern with scores >= 0.9
    if all(country == most_common_country for country in all_top1_countries) and all(
            score >= 0.8 for score in all_top1_scores):
        return most_common_country

    # Rule 2: All but one timezone show the same pattern with scores >= 0.7
    matching_countries = sum(country == most_common_country for country in all_top1_countries)
    if matching_countries == len(all_top1_countries) - 1 and all(score >= 0.7 for score in all_top1_scores):
        return most_common_country

    return None

