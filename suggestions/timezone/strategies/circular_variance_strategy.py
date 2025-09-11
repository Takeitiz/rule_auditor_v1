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
    Enhanced with detailed reason generation.
    """

    @requires_metrics('count_mtime_weekday_distribution',
                      'count_mtime_date_label_lag_distribution',
                      'count_30_min_distribution')
    def suggest_timezone(self,
                         statistics: StatisticsResult,
                         timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        """Generate timezone suggestion based on circular variance analysis"""

        if timezone:
            # Analyze specific timezone
            metrics = self._calculate_circular_metrics(statistics, timezone)
            if not metrics:
                return None

            result = TimezoneResult(timezone=timezone)
            result.method_used = "circular_variance"
            result.confidence = 1 - metrics['circular_variance']
            result.suggest_reason = self._generate_reason(timezone, metrics, None)
            return result

        # Analyze all timezones
        circular_metrics = {}

        for tz in statistics.count_30_min_distribution.keys():
            metrics = self._calculate_circular_metrics(statistics, tz)
            if metrics:
                circular_metrics[tz] = metrics

        if not circular_metrics:
            return None

        # Select timezone with highest concentration (lowest variance)
        best_tz = max(circular_metrics.keys(),
                      key=lambda k: circular_metrics[k]['concentration_score'])
        best_metrics = circular_metrics[best_tz]

        # Only suggest if concentration is significant
        if best_metrics['rayleigh_pvalue'] < 0.05:
            result = TimezoneResult(timezone=best_tz)
            result.method_used = "circular_variance"
            result.confidence = 1 - best_metrics['circular_variance']
            result.suggest_reason = self._generate_reason(best_tz, best_metrics, circular_metrics)
            return result

        return None

    def _calculate_circular_metrics(self,
                                    statistics: StatisticsResult,
                                    timezone: str) -> Optional[Dict]:
        """Calculate comprehensive circular statistics for a timezone"""

        time_dist = statistics.count_30_min_distribution.get(timezone, {})
        if not time_dist or len(time_dist) < 2:
            return None

        # Convert to angles
        angles = []
        weights = []

        for bucket, count in time_dist.items():
            hour = int(bucket[:2])
            minute = 30 if len(bucket) > 2 and bucket[2:] == '30' else 0
            time_hours = hour + minute / 60
            angle = (time_hours / 24) * 2 * np.pi
            angles.append(angle)
            weights.append(count)

        angles = np.array(angles)
        weights = np.array(weights) / sum(weights)

        # Calculate circular statistics
        mean_sin = np.sum(weights * np.sin(angles))
        mean_cos = np.sum(weights * np.cos(angles))
        circular_mean = np.arctan2(mean_sin, mean_cos)

        if circular_mean < 0:
            circular_mean += 2 * np.pi

        R = np.sqrt(mean_sin ** 2 + mean_cos ** 2)
        circular_variance = 1 - R
        circular_std = np.sqrt(-2 * np.log(R)) if R > 0 else np.pi

        # Von Mises concentration
        kappa = self._estimate_kappa(R)

        # Rayleigh test
        n = len(angles)
        rayleigh_z = n * R ** 2
        rayleigh_pvalue = np.exp(-rayleigh_z) * (1 + (2 * rayleigh_z - rayleigh_z ** 2) / (4 * n))

        # Multimodality detection
        is_multimodal = self._detect_multimodality(angles, weights)

        # Concentration score
        concentration_score = self._calculate_concentration_score(R, kappa, circular_variance, is_multimodal)

        # Peak time analysis
        peak_time_hours = (circular_mean / (2 * np.pi)) * 24

        return {
            'circular_mean': circular_mean,
            'circular_mean_hours': peak_time_hours,
            'circular_variance': circular_variance,
            'circular_std': circular_std,
            'circular_std_hours': (circular_std / (2 * np.pi)) * 24,
            'resultant_length': R,
            'concentration_parameter': kappa,
            'rayleigh_pvalue': rayleigh_pvalue,
            'is_multimodal': is_multimodal,
            'concentration_score': concentration_score,
            'num_time_points': len(angles)
        }

    def _estimate_kappa(self, R: float) -> float:
        """Estimate von Mises concentration parameter"""
        if R < 0.53:
            kappa = 2 * R + R ** 3 + 5 * R ** 5 / 6
        elif R < 0.85:
            kappa = -0.4 + 1.39 * R + 0.43 / (1 - R)
        else:
            kappa = 1 / (R ** 3 - 4 * R ** 2 + 3 * R) if R < 1 else 100
        return max(0, min(kappa, 100))

    def _detect_multimodality(self, angles: np.ndarray, weights: np.ndarray) -> bool:
        """Detect if distribution has multiple modes"""
        if len(angles) < 3:
            return False

        # Sort by angle
        sorted_idx = np.argsort(angles)
        sorted_weights = weights[sorted_idx]

        # Find peaks
        max_weight = np.max(sorted_weights)
        significant_peaks = sorted_weights > 0.2 * max_weight

        return np.sum(significant_peaks) > 1

    def _calculate_concentration_score(self, R: float, kappa: float,
                                       variance: float, is_multimodal: bool) -> float:
        """Calculate overall concentration score"""
        base_score = R

        if kappa > 2:
            base_score *= 1.5
        elif kappa > 1:
            base_score *= 1.2

        if is_multimodal:
            base_score *= 0.7

        if variance > 0.8:
            base_score *= 0.5
        elif variance > 0.6:
            base_score *= 0.8

        return min(base_score, 1.0)

    def _generate_reason(self,
                         timezone: str,
                         metrics: Dict,
                         all_metrics: Optional[Dict] = None) -> str:
        """Generate detailed reason for circular variance suggestion"""

        reasons = []

        # Time concentration
        mean_hours = metrics['circular_mean_hours']
        std_hours = metrics['circular_std_hours']
        reasons.append(f"Activity centered at {mean_hours:.1f}h ± {std_hours:.1f}h")

        # Variance interpretation
        variance = metrics['circular_variance']
        if variance < 0.1:
            reasons.append(f"Extremely concentrated (variance={variance:.3f})")
        elif variance < 0.3:
            reasons.append(f"Highly concentrated (variance={variance:.3f})")
        elif variance < 0.5:
            reasons.append(f"Moderately concentrated (variance={variance:.3f})")
        else:
            reasons.append(f"Dispersed activity (variance={variance:.3f})")

        # Concentration parameter
        kappa = metrics['concentration_parameter']
        if kappa > 10:
            reasons.append(f"Very strong concentration (κ={kappa:.1f})")
        elif kappa > 2:
            reasons.append(f"Strong concentration (κ={kappa:.1f})")
        elif kappa > 1:
            reasons.append(f"Moderate concentration (κ={kappa:.1f})")
        else:
            reasons.append(f"Weak concentration (κ={kappa:.1f})")

        # Multimodality
        if metrics['is_multimodal']:
            reasons.append("Multiple activity peaks detected")
        else:
            reasons.append("Single dominant peak")

        # Statistical significance
        pvalue = metrics['rayleigh_pvalue']
        if pvalue < 0.001:
            reasons.append("Highly significant non-uniform pattern (p<0.001)")
        elif pvalue < 0.01:
            reasons.append("Significant non-uniform pattern (p<0.01)")
        elif pvalue < 0.05:
            reasons.append("Non-uniform pattern (p<0.05)")

        # Comparative analysis
        if all_metrics and len(all_metrics) > 1:
            variances = [m['circular_variance'] for tz, m in all_metrics.items()]
            min_variance = min(variances)
            if variance == min_variance:
                reasons.append("Lowest circular variance among all timezones")

        return ". ".join(reasons)