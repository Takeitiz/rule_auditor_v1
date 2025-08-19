"""
Autocorrelation Strategy for Timezone Detection
"""

import numpy as np
from typing import Optional, Dict, List
from scipy import signal
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult


class AutocorrelationStrategy(TimezoneStrategy):
    """
    Detect timezone through autocorrelation of temporal patterns.
    Identifies periodicities at 24h and 168h (weekly) lags.
    """

    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[
        TimezoneResult]:
        """Generate timezone suggestion using autocorrelation analysis"""

        if timezone:
            # Analyze specific timezone
            score = self._calculate_autocorrelation_score(statistics, timezone)
            if score > 0.5:
                result = TimezoneResult(timezone=timezone)
                result.method_used = "autocorrelation"
                result.confidence = score
                return result
            return None

        # Analyze all timezones
        timezone_scores = {}

        for tz in statistics.count_30_min_distribution.keys():
            score = self._calculate_autocorrelation_score(statistics, tz)
            if score > 0:
                timezone_scores[tz] = score

        if not timezone_scores:
            return None

        # Select timezone with highest autocorrelation score
        best_tz = max(timezone_scores, key=timezone_scores.get)
        best_score = timezone_scores[best_tz]

        if best_score > 0.5:  # Minimum threshold
            result = TimezoneResult(timezone=best_tz)
            result.method_used = "autocorrelation"
            result.confidence = best_score

            # Get detailed ACF values for reasoning
            acf_details = self._get_acf_details(statistics, best_tz)

            result.suggest_reason = (
                f"Strong periodicity detected: "
                f"24h ACF={acf_details.get(24, 0):.3f}, "
                f"168h ACF={acf_details.get(168, 0):.3f}, "
                f"business pattern={acf_details.get('business_pattern', False)}"
            )
            return result

        return None

    def _calculate_autocorrelation_score(self, statistics: StatisticsResult, timezone: str) -> float:
        """Calculate comprehensive autocorrelation score for a timezone"""

        # Create time series from distribution
        time_series = self._create_hourly_series(statistics, timezone)

        if len(time_series) < 48:  # Need at least 2 days of data
            return 0

        # Calculate autocorrelation function
        acf = self._autocorrelation(time_series, max_lag=min(168, len(time_series) // 2))

        # Score based on peaks at expected lags
        score = 0
        weights = {
            24: 2.0,  # Daily pattern (most important)
            48: 1.5,  # 2-day pattern
            72: 1.0,  # 3-day pattern
            96: 1.0,  # 4-day pattern
            120: 1.5,  # 5-day pattern (business week)
            168: 1.5  # Weekly pattern
        }

        for lag, weight in weights.items():
            if lag < len(acf):
                # Consider positive autocorrelation only
                if acf[lag] > 0:
                    score += acf[lag] * weight

        # Normalize score
        total_weight = sum(weights.values())
        normalized_score = score / total_weight

        # Check for business day pattern (5-day cycle)
        if self._has_business_pattern(acf):
            normalized_score *= 1.2  # Boost score for business patterns

        return min(normalized_score, 1.0)

    def _create_hourly_series(self, statistics: StatisticsResult, timezone: str) -> np.ndarray:
        """Create hourly time series from 30-minute bucket distribution"""

        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist:
            return np.array([])

        # Create hourly aggregation
        hourly_counts = {}

        for bucket, count in time_dist.items():
            hour = int(bucket[:2])
            if hour not in hourly_counts:
                hourly_counts[hour] = 0
            hourly_counts[hour] += count

        # Extend to multiple days based on weekday distribution
        weekday_dist = statistics.count_weekday_distribution.get(timezone, {})

        # Create a week-long series
        series = []
        day_weights = self._get_day_weights(weekday_dist)

        for day in range(7):
            day_weight = day_weights[day]
            for hour in range(24):
                count = hourly_counts.get(hour, 0) * day_weight
                series.append(count)

        return np.array(series)

    def _get_day_weights(self, weekday_dist: Dict) -> List[float]:
        """Get relative weights for each day of the week"""

        if not weekday_dist:
            return [1.0] * 7

        day_map = {
            'sunday': 0, 'monday': 1, 'tuesday': 2, 'wednesday': 3,
            'thursday': 4, 'friday': 5, 'saturday': 6
        }

        weights = [0.5] * 7  # Default weight
        total = sum(weekday_dist.values())

        if total > 0:
            for day, count in weekday_dist.items():
                if day.lower() in day_map:
                    idx = day_map[day.lower()]
                    weights[idx] = count / (total / 7)  # Relative to average

        return weights

    def _autocorrelation(self, x: np.ndarray, max_lag: int) -> np.ndarray:
        """
        Compute normalized autocorrelation function.
        Uses FFT for efficiency with long series.
        """

        if len(x) < 2:
            return np.array([1.0])

        # Remove mean and normalize
        x = np.array(x, dtype=float)
        x = x - np.mean(x)

        # Use FFT for efficient computation
        # Pad to next power of 2 for FFT efficiency
        n = len(x)
        padded_len = 2 ** int(np.ceil(np.log2(2 * n - 1)))

        # Compute autocorrelation via FFT
        fft = np.fft.fft(x, n=padded_len)
        power_spectrum = np.abs(fft) ** 2
        acf_full = np.fft.ifft(power_spectrum).real

        # Normalize and extract relevant lags
        acf = acf_full[:max_lag + 1] / acf_full[0]

        return acf

    def _has_business_pattern(self, acf: np.ndarray) -> bool:
        """
        Check if ACF shows business day pattern.
        Look for peaks at 24h intervals with weekly reset.
        """

        if len(acf) < 120:  # Need at least 5 days
            return False

        # Check for 5-day pattern
        business_days = [24, 48, 72, 96, 120]  # Mon-Fri in hours
        weekend_days = [144, 168]  # Sat-Sun in hours

        business_acf = []
        weekend_acf = []

        for lag in business_days:
            if lag < len(acf):
                business_acf.append(acf[lag])

        for lag in weekend_days:
            if lag < len(acf):
                weekend_acf.append(acf[lag])

        if business_acf and weekend_acf:
            # Business days should have higher correlation than weekends
            business_mean = np.mean(business_acf)
            weekend_mean = np.mean(weekend_acf)

            return business_mean > weekend_mean * 1.5  # 50% higher correlation for business days

        return False

    def _get_acf_details(self, statistics: StatisticsResult, timezone: str) -> Dict:
        """Get detailed ACF values for reporting"""

        time_series = self._create_hourly_series(statistics, timezone)

        if len(time_series) < 48:
            return {}

        acf = self._autocorrelation(time_series, max_lag=min(168, len(time_series) // 2))

        details = {}
        key_lags = [24, 48, 72, 96, 120, 168]

        for lag in key_lags:
            if lag < len(acf):
                details[lag] = float(acf[lag])

        details['business_pattern'] = self._has_business_pattern(acf)

        # Find dominant period
        if len(acf) > 24:
            # Look for peaks in ACF
            peaks = signal.find_peaks(acf[1:], height=0.3)[0] + 1
            if len(peaks) > 0:
                details['dominant_period'] = int(peaks[0])

        return details