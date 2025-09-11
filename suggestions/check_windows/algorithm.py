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

        # time_dist = statistics.count_30_min_distribution.get(self.timezone, {})
        time_dist = statistics.count_non_updated_events_30_min_distribution.get(self.timezone, {})

        # Analyze distribution to determine best parameters
        dist_analysis = analyze_distribution(time_dist)

        # Adjust parameters based on distribution characteristics
        if dist_analysis.get('is_scattered', False):
            # For scattered distribution, increase alert penalty
            alert_penalty = 0.4
            coverage_threshold = 0.75
        elif dist_analysis.get('is_multimodal', False):
            # For multimodal, moderate penalty
            alert_penalty = 0.3
            coverage_threshold = 0.8
        else:
            # For tight distribution, lower penalty
            alert_penalty = 0.2
            coverage_threshold = 0.85

        # Calculate optimal time
        start_time_seconds, metrics = calculate_optimal_time_with_coverage(
            time_dist,
            lookback_hours=2.0,
            coverage_threshold=coverage_threshold,
            alert_penalty_weight=alert_penalty
        )

        # start_time = calculate_time_by_percentile_from_distribution(time_dist, 90)

        result.start_time = start_time_seconds
        # result.start_time = start_time
        result.end_time = 86340  # 23:59
        # result.metrics = metrics  # Store metrics for debugging/moniroting
        # result.distribution_analysis = dist_analysis

        holiday_result = suggest_holiday(statistics, self.timezone)
        result.holiday_calendar = holiday_result.get('country') if holiday_result else None
        result.day_offset = holiday_result.get('shift') if holiday_result else None

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


def suggest_holiday(statistics: StatisticsResult, timezone: str):
    """Generate holiday suggestion based on statistics"""
    holiday_similarity = statistics.holiday_metrics
    if not holiday_similarity:
        return None

    # Get data for the specific timezone
    timezone_data = holiday_similarity.get(timezone, {})
    if not timezone_data:
        return None

    # Filter out weekday entries and get only holiday entries
    holiday_entries = {
        key: value for key, value in timezone_data.items()
        if not key.startswith('weekday::') and key.startswith('holiday_')
    }

    if not holiday_entries:
        return None

    # Find the top scoring holiday entry
    sorted_matches = sorted(
        [(k, v) for k, v in holiday_entries.items()],
        key=lambda x: x[1].get('similarity_score', 0),
        reverse=True
    )

    if not sorted_matches:
        return None

    # Get the top match
    top_key, top_data = sorted_matches[0]
    similarity_score = top_data.get('similarity_score', 0)

    # Check if the top match meets the minimum threshold
    if similarity_score < 0.7:
        return None

    # Extract country and shift information
    country = top_data.get('country')
    shift_str = top_data.get('shift', '0')

    if not country:
        return None

    # Convert shift to integer
    try:
        # Handle formats like "+1", "-2", "0"
        shift_int = int(shift_str) if shift_str else 0
    except (ValueError, TypeError):
        # Fallback to 0 if conversion fails
        shift_int = 0

    return {
        'country': country,
        'shift': -shift_int,
    }


def calculate_optimal_time_with_coverage(
        distribution: Dict[str, float],
        lookback_hours: float = 2.0,
        coverage_threshold: float = 0.8,
        alert_penalty_weight: float = 0.3
) -> Tuple[Optional[int], dict]:
    """
    Calculate optimal start time considering coverage and alert burden.

    Args:
        distribution: Dict with time keys (e.g., "1030") and probability values
        lookback_hours: Hours to look back for coverage window (default 2)
        coverage_threshold: Minimum acceptable coverage (default 0.8)
        alert_penalty_weight: Weight for penalizing future alerts (0-1, higher = more penalty)

    Returns:
        Tuple of (time_in_seconds, metrics_dict)
    """

    if not distribution:
        return None, {}

    # Convert distribution to list of (time_in_minutes, probability) tuples
    time_prob_list = []
    for time_str, prob in distribution.items():
        hours = int(time_str[:2])
        minutes = int(time_str[2:])
        time_in_minutes = hours * 60 + minutes
        time_prob_list.append((time_in_minutes, prob))
    time_prob_list.sort()

    # Generate all possible 30-minute slots in a day (48 slots)
    all_slots = []
    for hour in range(24):
        for minute in [0, 30]:
            all_slots.append(hour * 60 + minute)

    # Calculate coverage and metrics for each possible start time
    lookback_minutes = int(lookback_hours * 60)
    results = []

    for slot_time in all_slots:
        # Calculate coverage (lookback window + everything after)
        coverage = 0.0
        forward_coverage = 0.0
        backward_coverage = 0.0
        alertts_expected = 0.0

        for time_min, prob in time_prob_list:
            if time_min >= slot_time:
                # Files arriving after chosen time
                coverage += prob
                forward_coverage += prob
                alertts_expected += prob  # These will trigger alerts
            elif time_min >= slot_time - lookback_minutes:
                # Files in lookback window (already arrived, no alerts)
                coverage += prob
                backward_coverage += prob

        # Calculate a score that balances coverage vs alert burden
        # Higher coverage is good, fewer alerts is good
        score = coverage - (alert_penalty_weight * alertts_expected)

        results.append({
            'time_minutes': slot_time,
            'time_str': f"{slot_time // 60:02d}{slot_time % 60:02d}",
            'coverage': coverage,
            'forward_coverage': forward_coverage,
            'backward_coverage': backward_coverage,
            'alerts_expected': alertts_expected,
            'score': score,
            'percentile': calculate_percentile_for_time(time_prob_list, slot_time)
        })

    # Sort by score (descending) then by time (descending for latest)
    results.sort(key=lambda x: (x['score'], x['time_minutes']), reverse=True)

    # Find the best option that meets coverage threshold
    best_result = None
    for result in results:
        if result['coverage'] >= coverage_threshold:
            best_result = result
            break

    # If no result meets threshold, take the one with best coverage
    if not best_result:
        results.sort(key=lambda x: (x['coverage'], -x['alerts_expected']), reverse=True)
        best_result = results[0]

    # Also find what pure coverage optimization would give
    coverage_optimal = max(results, key=lambda x: (x['coverage'], x['time_minutes']))

    # Find what the old percentile method would give
    percentile_90_time = calculate_time_by_percentile_from_distribution(distribution, 90)
    percentile_result = next((r for r in results if r['time_minutes'] * 60 == percentile_90_time), None)

    metrics = {
        'chosen_time': best_result['time_str'],
        'chosen_time_seconds': best_result['time_minutes'] * 60,
        'coverage': best_result['coverage'],
        'forward_coverage': best_result['forward_coverage'],
        'backward_coverage': best_result['backward_coverage'],
        'alerts_expected': best_result['alerts_expected'],
        'score': best_result['score'],
        'percentile': best_result['percentile'],
        'coverage_optimal_time': coverage_optimal['time_str'],
        'coverage_optimal_coverage': coverage_optimal['coverage'],
        'percentile_90_coverage': percentile_result['coverage'] if percentile_result else None,
        'all_resulst': results[:10]  # Top 10 options for debugging
    }

    return best_result['time_minutes'] * 60, metrics

def calculate_percentile_for_time(time_prob_list: List[Tuple[int, float]], target_time: int) -> float:
    """Calculate what percentile a given time represents in the distribution."""
    cumulative = 0.0
    for time_min, prob in time_prob_list:
        if time_min < target_time:
            cumulative += prob
    return cumulative * 100


