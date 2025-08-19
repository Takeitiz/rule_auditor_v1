"""
Circular Variance Strategy for Timezone Detection
Uses circular statistics to measure activity concentration.
"""

import numpy as np
from typing import Optional, Dict, Tuple
from scipy import stats
from scipy.special import i0
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult


class CircularVarianceStrategy(TimezoneStrategy):
    """
    Detect timezone using circular statistics.
    Low circular variance indicates concentrated activity (correct timezone).
    High circular variance indicates scattered activity (wrong timezone).
    """

    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        """Generate timezone suggestion based on circular variance analysis"""

        if timezone:
            # If specific timezone requested, only analyze that one
            metrics = self._calculate_circular_metrics(statistics, timezone)
            if metrics and metrics['confidence'] < 0.05:  # Significant non-uniform distribution
                result = TimezoneResult(timezone=timezone)
                result.method_used = "circular_variance"
                result.confidence = 1 - metrics['circular_variance']
                result.suggest_reason = (
                    f"Activity concentrated at {metrics['circular_mean_time']:.1f}h "
                    f"(variance={metrics['circular_variance']:.3f}, κ={metrics['concentration_parameter']:.2f})"
                )
                return result
            return None

        # Analyze all timezones
        circular_metrics = {}

        for tz in statistics.count_30_min_distribution.keys():
            metrics = self._calculate_circular_metrics(statistics, tz)

            if metrics:
                # Score based on concentration (inverse of variance)
                score = metrics['concentration_score']
                circular_metrics[tz] = {
                    'score': score,
                    'variance': metrics['circular_variance'],
                    'mean_time': metrics['circular_mean_time'],
                    'concentration': metrics['concentration_parameter'],
                    'confidence': metrics['rayleigh_pvalue'],
                    'is_multimodal': metrics['is_multimodal'],
                    'weekday_variance': metrics.get('weekday_variance', None)
                }

        if not circular_metrics:
            return None

        # Select timezone with highest concentration (lowest variance)
        best_tz = max(circular_metrics.keys(), key=lambda k: circular_metrics[k]['score'])
        best_metrics = circular_metrics[best_tz]

        # Only suggest if concentration is significant (p-value < 0.05 indicates non-uniform)
        if best_metrics['confidence'] < 0.05:
            result = TimezoneResult(timezone=best_tz)
            result.method_used = "circular_variance"
            result.confidence = 1 - best_metrics['variance']  # Convert variance to confidence
            result.suggest_reason = (
                f"Activity concentrated at {best_metrics['mean_time']:.1f}h "
                f"(variance={best_metrics['variance']:.3f}, κ={best_metrics['concentration']:.2f}, "
                f"multimodal={best_metrics['is_multimodal']})"
            )
            return result

        return None

    def _calculate_circular_metrics(self, statistics: StatisticsResult, timezone: str) -> Optional[Dict]:
        """Calculate comprehensive circular statistics for a timezone"""

        # Extract time distribution
        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist or len(time_dist) < 4:  # Need minimum data points
            return None

        # Convert to angles (0-24h maps to 0-2π)
        angles = []
        weights = []

        for bucket, count in time_dist.items():
            hour = int(bucket[:2])
            minute = 30 if bucket[2:] == '30' else 0
            time_hours = hour + minute/60

            # Convert to radians
            angle = (time_hours / 24) * 2 * np.pi
            angles.append(angle)
            weights.append(count)

        angles = np.array(angles)
        weights = np.array(weights)

        # Normalize weights
        weights = weights / weights.sum()

        # Calculate circular mean
        mean_sin = np.sum(weights * np.sin(angles))
        mean_cos = np.sum(weights * np.cos(angles))
        circular_mean = np.arctan2(mean_sin, mean_cos)

        # Ensure positive angle
        if circular_mean < 0:
            circular_mean += 2 * np.pi

        # Calculate resultant length (R)
        R = np.sqrt(mean_sin**2 + mean_cos**2)

        # Circular variance (0 = all concentrated, 1 = uniform)
        circular_variance = 1 - R

        # Circular standard deviation
        circular_std = np.sqrt(-2 * np.log(R)) if R > 0 else np.pi

        # Von Mises concentration parameter (κ)
        kappa = self._estimate_kappa(R)

        # Calculate confidence intervals for mean
        confidence_95 = self._circular_confidence_interval(circular_mean, R, len(angles))

        # Rayleigh test for uniformity (null: uniform distribution)
        rayleigh_z = len(angles) * R**2
        rayleigh_pvalue = np.exp(-rayleigh_z) * (1 + (2*rayleigh_z - rayleigh_z**2)/(4*len(angles)))

        # Multi-modal detection
        is_multimodal = self._detect_multimodality(angles, weights)

        # Calculate concentration score
        concentration_score = self._calculate_concentration_score(
            R, kappa, circular_variance, is_multimodal
        )

        # Check weekday patterns using circular variance
        weekday_variance = self._calculate_weekday_circular_variance(statistics, timezone)

        return {
            'circular_mean': circular_mean,
            'circular_mean_time': (circular_mean / (2 * np.pi)) * 24,  # Convert back to hours
            'circular_variance': circular_variance,
            'circular_std': circular_std,
            'resultant_length': R,
            'concentration_parameter': kappa,
            'confidence_interval': confidence_95,
            'rayleigh_pvalue': rayleigh_pvalue,
            'is_multimodal': is_multimodal,
            'concentration_score': concentration_score,
            'weekday_variance': weekday_variance
        }

    def _estimate_kappa(self, R: float) -> float:
        """
        Estimate von Mises concentration parameter using ML approximation.
        Higher κ indicates more concentrated distribution.
        """
        if R < 0.53:
            kappa = 2 * R + R**3 + 5 * R**5 / 6
        elif R < 0.85:
            kappa = -0.4 + 1.39 * R + 0.43 / (1 - R)
        else:
            kappa = 1 / (R**3 - 4 * R**2 + 3 * R)

        return max(0, kappa)  # Ensure non-negative

    def _circular_confidence_interval(self, mean: float, R: float, n: int, alpha: float = 0.05) -> Tuple[float, float]:
        """Calculate confidence interval for circular mean"""
        from scipy.stats import chi2

        if R < 0.9 and n >= 25:
            # Use normal approximation for moderate concentration
            z = stats.norm.ppf(1 - alpha/2)
            delta = z * np.sqrt((1 - R**2) / (n * R**2))
        else:
            # Use chi-square approximation for high concentration
            chi2_val = chi2.ppf(1 - alpha, df=1)
            if R > 0:
                arg = 1 - chi2_val / (2 * n * R**2)
                # Ensure valid argument for arccos
                arg = max(-1, min(1, arg))
                delta = np.arccos(arg)
            else:
                delta = np.pi

        return (mean - delta, mean + delta)

    def _detect_multimodality(self, angles: np.ndarray, weights: np.ndarray) -> bool:
        """
        Detect if distribution has multiple modes using kernel density estimation.
        Multiple modes might indicate activity across multiple timezones.
        """
        from scipy.stats import vonmises

        # Create circular KDE
        R = self._calculate_R(angles, weights)
        bandwidth = 1.06 * np.sqrt(1 - R) * len(angles)**(-1/5) if R < 1 else 0.1

        # Evaluate density at regular points
        test_points = np.linspace(0, 2*np.pi, 100)
        density = np.zeros_like(test_points)

        for angle, weight in zip(angles, weights):
            # Von Mises kernel
            kappa = 1 / (bandwidth**2 + 0.01)  # Add small constant to avoid division by zero
            density += weight * vonmises.pdf(test_points, kappa, loc=angle)

        # Find peaks (local maxima)
        peaks = []
        for i in range(1, len(density)-1):
            if density[i] > density[i-1] and density[i] > density[i+1]:
                # Check if peak is significant (at least 10% of max density)
                if density[i] > 0.1 * np.max(density):
                    peaks.append(i)

        return len(peaks) > 1

    def _calculate_R(self, angles: np.ndarray, weights: Optional[np.ndarray] = None) -> float:
        """Calculate resultant length R"""
        if weights is None:
            weights = np.ones_like(angles) / len(angles)

        mean_sin = np.sum(weights * np.sin(angles))
        mean_cos = np.sum(weights * np.cos(angles))
        return np.sqrt(mean_sin**2 + mean_cos**2)

    def _calculate_concentration_score(self, R: float, kappa: float, variance: float, is_multimodal: bool) -> float:
        """
        Calculate overall concentration score for timezone ranking.
        Higher score = better timezone candidate.
        """
        # Base score from resultant length (0 to 1)
        base_score = R

        # Boost for high concentration
        if kappa > 2:  # Strong concentration
            base_score *= 1.5
        elif kappa > 1:  # Moderate concentration
            base_score *= 1.2

        # Penalty for multimodal distributions (might indicate mixed timezones)
        if is_multimodal:
            base_score *= 0.7

        # Penalty for very high variance (nearly uniform distribution)
        if variance > 0.8:
            base_score *= 0.5
        elif variance > 0.6:
            base_score *= 0.8

        return min(base_score, 1.0)  # Cap at 1.0

    def _calculate_weekday_circular_variance(self, statistics: StatisticsResult, timezone: str) -> Optional[float]:
        """Calculate circular variance for weekday patterns"""
        weekday_dist = statistics.count_weekday_distribution.get(timezone, {})
        if not weekday_dist:
            return None

        # Map weekdays to angles (0-7 days maps to 0-2π)
        weekday_map = {
            'sunday': 0, 'monday': 1, 'tuesday': 2, 'wednesday': 3,
            'thursday': 4, 'friday': 5, 'saturday': 6
        }

        angles = []
        weights = []

        for day, count in weekday_dist.items():
            if day.lower() in weekday_map:
                angle = (weekday_map[day.lower()] / 7) * 2 * np.pi
                angles.append(angle)
                weights.append(count)

        if not angles:
            return None

        weights = np.array(weights) / sum(weights)
        R = self._calculate_R(np.array(angles), weights)

        return 1 - R  # Circular variance