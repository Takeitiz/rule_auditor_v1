"""
Cross-Correlation Pattern Matching Strategy
"""

import numpy as np
from typing import Optional, Dict, List, Tuple
from scipy import signal
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.const import TIMEZONE_MAP


class CrossCorrelationStrategy(TimezoneStrategy):
    """
    Match patterns against reference timezone behaviors using cross-correlation.
    Detects timezone by finding the best match with known patterns.
    """

    def __init__(self):
        super().__init__()
        self.reference_patterns = self._create_reference_patterns()

    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[
        TimezoneResult]:
        """Generate timezone suggestion using cross-correlation pattern matching"""

        if timezone:
            # Match against specific timezone reference
            if timezone in self.reference_patterns:
                correlation = self._match_pattern(statistics, timezone)
                if correlation > 0.7:
                    result = TimezoneResult(timezone=timezone)
                    result.method_used = "cross_correlation"
                    result.confidence = correlation
                    return result
            return None

        # Match against all reference patterns
        best_match = None
        max_correlation = 0
        correlation_details = {}

        for tz in statistics.count_30_min_distribution.keys():
            pattern = self._extract_pattern(statistics, tz)

            if len(pattern) < 48:  # Need at least one day of data
                continue

            for ref_tz, ref_pattern in self.reference_patterns.items():
                # Calculate normalized cross-correlation
                correlation, lag = self._cross_correlate(pattern, ref_pattern)

                if correlation > max_correlation:
                    max_correlation = correlation
                    best_match = tz
                    best_ref = ref_tz
                    best_lag = lag

                correlation_details[f"{tz}->{ref_tz}"] = {
                    'correlation': correlation,
                    'lag': lag
                }

        if best_match and max_correlation > 0.7:
            result = TimezoneResult(timezone=best_match)
            result.method_used = "cross_correlation"
            result.confidence = float(max_correlation)

            # Convert lag to hours for interpretation
            offset_hours = best_lag * 0.5  # 30-min buckets

            result.suggest_reason = (
                f"Pattern matches {best_ref} reference "
                f"(correlation={max_correlation:.3f}, offset={offset_hours:.1f}h)"
            )
            return result

        return None

    def _create_reference_patterns(self) -> Dict[str, np.ndarray]:
        """Create reference activity patterns for known timezones"""

        patterns = {}

        # Create typical business hour patterns for major timezones
        for tz_name, tz_full in TIMEZONE_MAP.items():
            pattern = self._generate_timezone_pattern(tz_full)
            patterns[tz_full] = pattern

        return patterns

    def _generate_timezone_pattern(self, timezone: str) -> np.ndarray:
        """
        Generate typical activity pattern for a timezone.
        Creates a 48-point pattern (30-min buckets for 24 hours).
        """

        pattern = np.zeros(48)

        # Define typical business patterns
        if timezone == 'America/New_York':
            # NYSE pattern: 9:30 AM - 4:00 PM ET
            business_start = 19  # 9:30 AM in 30-min buckets
            business_end = 32  # 4:00 PM
            lunch_dip = [24, 25]  # 12:00-1:00 PM

        elif timezone == 'Europe/London':
            # LSE pattern: 8:00 AM - 4:30 PM GMT
            business_start = 16  # 8:00 AM
            business_end = 33  # 4:30 PM
            lunch_dip = [24, 25]  # 12:00-1:00 PM

        elif timezone == 'Asia/Tokyo':
            # TSE pattern: 9:00 AM - 3:00 PM JST with lunch break
            business_start = 18  # 9:00 AM
            business_end = 30  # 3:00 PM
            lunch_dip = [23, 24, 25]  # 11:30 AM - 12:30 PM (TSE lunch break)

        else:  # GMT or default
            # Generic business pattern
            business_start = 18  # 9:00 AM
            business_end = 34  # 5:00 PM
            lunch_dip = [24, 25]  # 12:00-1:00 PM

        # Build the pattern
        for i in range(48):
            if business_start <= i < business_end:
                if i in lunch_dip:
                    pattern[i] = 0.7  # Reduced activity during lunch
                else:
                    pattern[i] = 1.0  # Full activity during business hours
            else:
                pattern[i] = 0.1  # Minimal activity outside business hours

        # Add some noise for realism
        noise = np.random.normal(0, 0.05, 48)
        pattern = pattern + noise
        pattern = np.clip(pattern, 0, 1)

        # Smooth the pattern
        pattern = signal.savgol_filter(pattern, window_length=5, polyorder=2)

        return pattern

    def _extract_pattern(self, statistics: StatisticsResult, timezone: str) -> np.ndarray:
        """Extract activity pattern from statistics for a timezone"""

        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist:
            return np.array([])

        # Create 48-point pattern (30-min buckets)
        pattern = np.zeros(48)

        for bucket, count in time_dist.items():
            hour = int(bucket[:2])
            minute = 30 if bucket[2:] == '30' else 0
            idx = hour * 2 + (1 if minute == 30 else 0)
            if idx < 48:
                pattern[idx] = count

        # Normalize pattern
        if pattern.max() > 0:
            pattern = pattern / pattern.max()

        return pattern

    def _cross_correlate(self, pattern1: np.ndarray, pattern2: np.ndarray) -> Tuple[float, int]:
        """
        Calculate normalized cross-correlation between two patterns.
        Returns (max_correlation, lag_at_max).
        """

        if len(pattern1) != len(pattern2):
            # Resize to same length
            min_len = min(len(pattern1), len(pattern2))
            pattern1 = pattern1[:min_len]
            pattern2 = pattern2[:min_len]

        # Normalize patterns
        pattern1 = (pattern1 - np.mean(pattern1)) / (np.std(pattern1) + 1e-10)
        pattern2 = (pattern2 - np.mean(pattern2)) / (np.std(pattern2) + 1e-10)

        # Calculate cross-correlation
        correlation = signal.correlate(pattern1, pattern2, mode='same', method='fft')

        # Normalize by lengths
        norm_factor = np.sqrt(np.sum(pattern1 ** 2) * np.sum(pattern2 ** 2))
        if norm_factor > 0:
            correlation = correlation / norm_factor

        # Find peak correlation
        max_corr_idx = np.argmax(np.abs(correlation))
        max_correlation = correlation[max_corr_idx]

        # Calculate lag (positive = pattern1 leads pattern2)
        lag = max_corr_idx - len(correlation) // 2

        return float(np.abs(max_correlation)), int(lag)

    def _match_pattern(self, statistics: StatisticsResult, timezone: str) -> float:
        """Match a specific timezone pattern against reference"""

        pattern = self._extract_pattern(statistics, timezone)

        if len(pattern) < 48 or timezone not in self.reference_patterns:
            return 0

        ref_pattern = self.reference_patterns[timezone]
        correlation, _ = self._cross_correlate(pattern, ref_pattern)

        return correlation