"""
Final versions of three timezone detection strategies
All with comprehensive suggest_reason generation
"""

import numpy as np
import math
from typing import Dict, Optional, List, Tuple
from scipy import stats
from scipy.special import i0
from scipy.stats import chi2, vonmises
from dataclasses import dataclass

# Assuming these are your existing base classes
from datnguyen.rule_auditor.suggestions.timezone.strategies.base import TimezoneStrategy
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.base import requires_metrics


# ==============================================================================
# 1. STABILITY-BASED STRATEGY
# ==============================================================================

class StabilityBasedStrategy(TimezoneStrategy):
    """
    Pattern-agnostic timezone detection based on stability metrics.
    Finds the timezone that makes data most stable and predictable.
    """

    def __init__(self,
                 stability_weights: Optional[Dict[str, float]] = None):
        """
        Initialize with optional custom weights for stability metrics.

        Args:
            stability_weights: Dict of metric_name -> weight
        """
        self.weights = stability_weights or {
            'concentration': 0.4,  # How concentrated the data is
            'predictability': 0.3,  # How predictable/low entropy
            'simplicity': 0.3,  # How few states are active
        }

    @requires_metrics('count_mtime_weekday_distribution',
                      'count_mtime_date_label_lag_distribution',
                      'count_30_min_distribution')
    def suggest_timezone(self,
                         statistics: StatisticsResult,
                         timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        """Generate timezone suggestion based on stability analysis"""

        weekday_dist = statistics.count_mtime_weekday_distribution
        lag_dist = statistics.count_mtime_date_label_lag_distribution
        time_dist = statistics.count_30_min_distribution

        if timezone:
            # Analyze specific timezone
            if timezone not in time_dist:
                return None

            metrics = self._calculate_stability_metrics(
                weekday_dist.get(timezone, {}),
                time_dist.get(timezone, {}),
                lag_dist.get(timezone, {})
            )

            result = TimezoneResult(timezone=timezone)
            result.method_used = "stability_based"
            result.confidence = metrics['overall_stability']
            result.suggest_reason = self._generate_reason(timezone, metrics, None)
            return result

        # Analyze all timezones
        all_metrics = {}
        for tz in time_dist.keys():
            metrics = self._calculate_stability_metrics(
                weekday_dist.get(tz, {}),
                time_dist.get(tz, {}),
                lag_dist.get(tz, {})
            )
            all_metrics[tz] = metrics

        if not all_metrics:
            return None

        # Find most stable timezone
        best_tz = max(all_metrics.keys(),
                      key=lambda tz: all_metrics[tz]['overall_stability'])
        best_metrics = all_metrics[best_tz]

        # Create result
        result = TimezoneResult(timezone=best_tz)
        result.method_used = "stability_based"
        result.confidence = best_metrics['overall_stability']
        result.suggest_reason = self._generate_reason(best_tz, best_metrics, all_metrics)

        return result

    def _calculate_stability_metrics(self,
                                     weekday: Dict,
                                     time_30min: Dict,
                                     lag: Dict) -> Dict:
        """Calculate comprehensive stability metrics"""

        # Analyze each distribution
        time_analysis = self._analyze_distribution(time_30min, max_bins=48)
        weekday_analysis = self._analyze_distribution(weekday, max_bins=7)
        lag_analysis = self._analyze_distribution(lag, max_bins=10)

        # Calculate aggregate scores
        concentration_score = np.mean([
            time_analysis['herfindahl'],
            time_analysis['simpson'],
            time_analysis['berger_parker']
        ])

        predictability_score = 1 - np.mean([
            time_analysis['normalized_entropy'],
            time_analysis['perplexity'] / 48  # Normalize perplexity
        ])

        simplicity_score = 1 - (time_analysis['active_bins'] / 48)

        # Overall stability
        overall = (
                self.weights['concentration'] * concentration_score +
                self.weights['predictability'] * predictability_score +
                self.weights['simplicity'] * simplicity_score
        )

        # Bonus for extreme concentration
        if time_analysis['peak_concentration'] > 0.9:
            overall *= 1.15
        elif time_analysis['peak_concentration'] > 0.8:
            overall *= 1.1

        return {
            'overall_stability': min(overall, 1.0),
            'concentration_score': concentration_score,
            'predictability_score': predictability_score,
            'simplicity_score': simplicity_score,
            'time_metrics': time_analysis,
            'weekday_metrics': weekday_analysis,
            'lag_metrics': lag_analysis
        }

    def _analyze_distribution(self, distribution: Dict, max_bins: int) -> Dict:
        """Analyze a single distribution for stability metrics"""

        if not distribution:
            return self._empty_analysis()

        values = np.array(list(distribution.values()))
        total = values.sum()

        if total == 0:
            return self._empty_analysis()

        probs = values / total
        nonzero_probs = probs[probs > 0]

        # Concentration indices
        herfindahl = np.sum(probs ** 2)
        simpson = herfindahl
        berger_parker = np.max(probs) if len(probs) > 0 else 0

        # Entropy metrics
        entropy = -np.sum(nonzero_probs * np.log2(nonzero_probs)) if len(nonzero_probs) > 0 else 0
        max_entropy = np.log2(max_bins) if max_bins > 1 else 1
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0

        # Perplexity
        perplexity = 2 ** entropy if entropy > 0 else 1

        # Concentration metrics
        sorted_values = np.sort(values)[::-1]
        peak_concentration = sorted_values[0] / total if len(sorted_values) > 0 else 0
        top3_concentration = sorted_values[:3].sum() / total if len(sorted_values) >= 3 else peak_concentration

        return {
            'herfindahl': herfindahl,
            'simpson': simpson,
            'berger_parker': berger_parker,
            'entropy': entropy,
            'normalized_entropy': normalized_entropy,
            'perplexity': perplexity,
            'peak_concentration': peak_concentration,
            'top3_concentration': top3_concentration,
            'active_bins': np.count_nonzero(values)
        }

    def _empty_analysis(self) -> Dict:
        """Return empty analysis metrics"""
        return {
            'herfindahl': 0, 'simpson': 0, 'berger_parker': 0,
            'entropy': 0, 'normalized_entropy': 0, 'perplexity': 1,
            'peak_concentration': 0, 'top3_concentration': 0, 'active_bins': 0
        }

    def _generate_reason(self,
                         timezone: str,
                         metrics: Dict,
                         all_metrics: Optional[Dict] = None) -> str:
        """Generate detailed reason for timezone suggestion"""

        reasons = []
        time_metrics = metrics['time_metrics']

        # Overall assessment
        stability = metrics['overall_stability']
        if stability > 0.8:
            reasons.append(f"Excellent stability score ({stability:.2f})")
        elif stability > 0.6:
            reasons.append(f"Good stability score ({stability:.2f})")
        else:
            reasons.append(f"Moderate stability score ({stability:.2f})")

        # Concentration analysis
        peak_conc = time_metrics['peak_concentration']
        if peak_conc > 0.9:
            reasons.append(f"Extremely concentrated: {peak_conc:.1%} in single time slot")
        elif peak_conc > 0.7:
            reasons.append(f"Highly concentrated: {peak_conc:.1%} in peak time")
        elif peak_conc > 0.5:
            reasons.append(f"Moderately concentrated: top slot has {peak_conc:.1%}")
        else:
            reasons.append(f"Distributed pattern: peak only {peak_conc:.1%}")

        # Predictability
        entropy = time_metrics['normalized_entropy']
        if entropy < 0.2:
            reasons.append(f"Very predictable (entropy={entropy:.3f})")
        elif entropy < 0.5:
            reasons.append(f"Predictable (entropy={entropy:.3f})")
        else:
            reasons.append(f"Less predictable (entropy={entropy:.3f})")

        # Simplicity
        active = time_metrics['active_bins']
        reasons.append(f"Activity in {active} time slots")

        # Comparative analysis if available
        if all_metrics and len(all_metrics) > 1:
            other_stabilities = [m['overall_stability'] for tz, m in all_metrics.items() if tz != timezone]
            if stability > max(other_stabilities) * 1.2:
                reasons.append("Significantly more stable than other timezones")
            elif stability > max(other_stabilities):
                reasons.append("Most stable among all timezones")

        # Key metrics summary
        reasons.append(f"Herfindahl index={time_metrics['herfindahl']:.3f}")

        return ". ".join(reasons)