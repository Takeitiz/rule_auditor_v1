"""
Mutual Information Strategy for Timezone Detection
"""

import numpy as np
from typing import Optional, Dict, List
from sklearn.metrics import mutual_info_score
from sklearn.feature_selection import mutual_info_regression
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult


class MutualInformationStrategy(TimezoneStrategy):
    """
    Use mutual information to detect temporal dependencies.
    More powerful than entropy as it captures non-linear relationships.
    """

    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[
        TimezoneResult]:
        """Generate timezone suggestion using mutual information analysis"""

        if timezone:
            # Analyze specific timezone
            mi_score = self._calculate_mi_score(statistics, timezone)
            if mi_score > 0.3:  # Threshold for significant MI
                result = TimezoneResult(timezone=timezone)
                result.method_used = "mutual_information"
                result.confidence = min(mi_score, 1.0)
                return result
            return None

        # Analyze all timezones
        mi_scores = {}

        for tz in statistics.count_weekday_distribution.keys():
            score = self._calculate_mi_score(statistics, tz)
            if score > 0:
                mi_scores[tz] = score

        if not mi_scores:
            return None

        # Select timezone with highest mutual information
        best_tz = max(mi_scores, key=mi_scores.get)
        best_score = mi_scores[best_tz]

        if best_score > 0.3:  # Minimum threshold
            result = TimezoneResult(timezone=best_tz)
            result.method_used = "mutual_information"
            result.confidence = min(best_score, 1.0)

            # Calculate additional MI metrics for reasoning
            lag_mi = self._calculate_lagged_mi(statistics, best_tz)
            weekday_mi = self._calculate_weekday_mi(statistics, best_tz)

            result.suggest_reason = (
                f"High temporal dependency (MI={best_score:.3f}), "
                f"24h lag MI={lag_mi.get(24, 0):.3f}, "
                f"weekday pattern MI={weekday_mi:.3f}"
            )
            return result

        return None

    def _calculate_mi_score(self, statistics: StatisticsResult, timezone: str) -> float:
        """Calculate comprehensive mutual information score"""

        # Get time distribution
        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist or len(time_dist) < 4:
            return 0

        # Convert to arrays for MI calculation
        times, counts = self._prepare_mi_data(time_dist)

        if len(times) < 10:
            return 0

        # Calculate MI between time-of-day and activity
        base_mi = self._calculate_base_mi(times, counts)

        # Calculate MI with lagged versions (detect periodicity)
        lag_mi = self._calculate_lagged_mi_values(times, counts)

        # Calculate conditional MI (time given day-of-week)
        conditional_mi = self._calculate_conditional_mi(statistics, timezone)

        # Combine scores with weights
        combined_score = (
            base_mi * 0.4 +
            max(lag_mi.values()) * 0.3 if lag_mi else 0 +
                                                      conditional_mi * 0.3
        )

        return combined_score

    def _prepare_mi_data(self, time_dist: Dict) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare data for MI calculation"""

        times = []
        counts = []

        for bucket, count in time_dist.items():
            hour = int(bucket[:2])
            minute = 30 if bucket[2:] == '30' else 0
            time_val = hour + minute / 60

            # Expand data points based on count
            # This preserves the distribution for MI calculation
            n_samples = min(int(count), 100)  # Cap to avoid memory issues
            times.extend([time_val] * n_samples)
            counts.extend([1] * n_samples)

        return np.array(times), np.array(counts)

    def _calculate_base_mi(self, times: np.ndarray, counts: np.ndarray) -> float:
        """Calculate base mutual information between time and activity"""

        if len(times) < 2:
            return 0

        # Discretize times into bins for MI calculation
        time_bins = np.digitize(times, bins=np.linspace(0, 24, 25))

        # Calculate mutual information
        try:
            mi = mutual_info_score(time_bins, counts)

            # Normalize by entropy for comparability
            time_entropy = self._entropy(time_bins)
            count_entropy = self._entropy(counts)
            max_entropy = min(time_entropy, count_entropy)

            if max_entropy > 0:
                normalized_mi = mi / max_entropy
                return float(normalized_mi)
        except:
            return 0

        return 0

    def _calculate_lagged_mi_values(self, times: np.ndarray, counts: np.ndarray) -> Dict[int, float]:
        """Calculate MI with lagged versions to detect periodicity"""

        lag_mi = {}

        for lag_hours in [6, 12, 24, 48]:
            if len(times) > lag_hours * 2:
                # Create lagged version
                lag_samples = int(lag_hours * 2)  # 2 samples per hour (30-min buckets)

                if lag_samples < len(counts):
                    original = counts[:-lag_samples]
                    lagged = counts[lag_samples:]

                    if len(original) > 0 and len(lagged) > 0:
                        try:
                            mi = mutual_info_score(
                                np.digitize(original, bins=np.percentile(original, [25, 50, 75])),
                                np.digitize(lagged, bins=np.percentile(lagged, [25, 50, 75]))
                            )
                            lag_mi[lag_hours] = mi
                        except:
                            lag_mi[lag_hours] = 0

        return lag_mi

    def _calculate_lagged_mi(self, statistics: StatisticsResult, timezone: str) -> Dict[int, float]:
        """Public method to calculate lagged MI for a timezone"""

        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist:
            return {}

        times, counts = self._prepare_mi_data(time_dist)
        return self._calculate_lagged_mi_values(times, counts)

    def _calculate_weekday_mi(self, statistics: StatisticsResult, timezone: str) -> float:
        """Calculate MI between weekday and activity pattern"""

        weekday_dist = statistics.count_weekday_distribution.get(timezone, {})
        if not weekday_dist:
            return 0

        # Create weekday and count arrays
        weekdays = []
        counts = []

        day_map = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                   'friday': 4, 'saturday': 5, 'sunday': 6}

        for day, count in weekday_dist.items():
            if day.lower() in day_map:
                # Expand based on count
                n_samples = min(int(count), 100)
                weekdays.extend([day_map[day.lower()]] * n_samples)
                counts.extend([1] * n_samples)

        if len(weekdays) < 7:
            return 0

        try:
            mi = mutual_info_score(weekdays, counts)
            # Normalize
            weekday_entropy = self._entropy(weekdays)
            if weekday_entropy > 0:
                return mi / weekday_entropy
        except:
            return 0

        return 0

    def _calculate_conditional_mi(self, statistics: StatisticsResult, timezone: str) -> float:
        """Calculate conditional mutual information I(time; activity | day_of_week)"""

        # This is a simplified version
        # Full implementation would require joint distributions

        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        weekday_dist = statistics.count_weekday_distribution.get(timezone, {})

        if not time_dist or not weekday_dist:
            return 0

        # Calculate MI for each day type (weekday vs weekend)
        weekday_values = []
        weekend_values = []

        for day, count in weekday_dist.items():
            if day.lower() in ['saturday', 'sunday']:
                weekend_values.append(count)
            else:
                weekday_values.append(count)

        # If there's a strong difference between weekday and weekend patterns
        if weekday_values and weekend_values:
            weekday_mean = np.mean(weekday_values)
            weekend_mean = np.mean(weekend_values)

            if weekday_mean > 0:
                ratio = abs(weekday_mean - weekend_mean) / weekday_mean
                return min(ratio, 1.0)

        return 0

    def _entropy(self, data: np.ndarray) -> float:
        """Calculate Shannon entropy"""

        if len(data) == 0:
            return 0

        # Get probability distribution
        unique, counts = np.unique(data, return_counts=True)
        probs = counts / counts.sum()

        # Calculate entropy
        entropy = -np.sum(probs * np.log2(probs + 1e-10))

        return entropy