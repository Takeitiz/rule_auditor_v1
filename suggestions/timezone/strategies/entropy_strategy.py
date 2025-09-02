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

    class ImprovedEntropyStrategy(TimezoneStrategy):
        """
        Enhanced entropy-based timezone strategy with detailed reasoning.
        Now includes all distributions with proper weighting.
        """

        def __init__(self,
                     time_weight: float = 0.6,
                     weekday_weight: float = 0.2,
                     lag_weight: float = 0.2):
            """
            Initialize with configurable weights.

            Args:
                time_weight: Weight for 30-minute time distribution
                weekday_weight: Weight for weekday distribution
                lag_weight: Weight for lag distribution
            """
            self.time_weight = time_weight
            self.weekday_weight = weekday_weight
            self.lag_weight = lag_weight

        @requires_metrics('count_mtime_weekday_distribution',
                          'count_mtime_date_label_lag_distribution',
                          'count_30_min_distribution')
        def suggest_timezone(self,
                             statistics: StatisticsResult,
                             timezone: Optional[str] = None) -> Optional[TimezoneResult]:
            """Generate timezone suggestion based on comprehensive entropy analysis"""

            weekday_dist = statistics.count_mtime_weekday_distribution
            lag_dist = statistics.count_mtime_date_label_lag_distribution
            time_dist = statistics.count_30_min_distribution

            if timezone:
                # Analyze specific timezone
                if (timezone not in weekday_dist or
                        timezone not in lag_dist or
                        timezone not in time_dist):
                    return None

                metrics = self._calculate_timezone_metrics(
                    weekday_dist.get(timezone, {}),
                    time_dist.get(timezone, {}),
                    lag_dist.get(timezone, {})
                )

                result = TimezoneResult(timezone=timezone)
                result.method_used = "improved_entropy"
                result.confidence = 1 - metrics['weighted_entropy']
                result.suggest_reason = self._generate_reason(timezone, metrics, None)
                return result

            # Analyze all timezones
            entropy_scores = {}
            detailed_metrics = {}

            for tz in set(weekday_dist.keys()) & set(lag_dist.keys()) & set(time_dist.keys()):
                metrics = self._calculate_timezone_metrics(
                    weekday_dist.get(tz, {}),
                    time_dist.get(tz, {}),
                    lag_dist.get(tz, {})
                )
                entropy_scores[tz] = metrics['weighted_entropy']
                detailed_metrics[tz] = metrics

            if not entropy_scores:
                return None

            # Find timezone with minimum entropy
            min_entropy = min(entropy_scores.values())
            best_timezones = [tz for tz, entropy in entropy_scores.items()
                              if abs(entropy - min_entropy) < 0.001]

            if len(best_timezones) == 1:
                best_tz = best_timezones[0]
            else:
                # Tie-breaker: use concentration
                best_tz = self._tiebreaker_by_concentration(time_dist, best_timezones)
                if not best_tz:
                    best_tz = best_timezones[0]

            # Create result
            result = TimezoneResult(timezone=best_tz)
            result.method_used = "improved_entropy"
            result.confidence = 1 - entropy_scores[best_tz]
            result.suggest_reason = self._generate_reason(best_tz, detailed_metrics[best_tz], detailed_metrics)

            return result

        def _calculate_timezone_metrics(self,
                                        weekday: Dict,
                                        time_30min: Dict,
                                        lag: Dict) -> Dict:
            """Calculate comprehensive entropy metrics"""

            # Calculate raw entropies
            time_entropy = self._calculate_entropy(time_30min)
            weekday_entropy = self._calculate_entropy(weekday)
            lag_entropy = self._calculate_entropy(lag)

            # Normalize entropies
            time_normalized = self._normalize_entropy(time_entropy, 48)
            weekday_normalized = self._normalize_entropy(weekday_entropy, 7)
            lag_normalized = self._normalize_entropy(lag_entropy, max(2, len(lag)))

            # Calculate additional metrics
            time_gini = self._calculate_gini(list(time_30min.values()))
            time_concentration = self._calculate_concentration(time_30min)

            # Weighted combination
            weighted_entropy = (
                    self.time_weight * time_normalized +
                    self.weekday_weight * weekday_normalized +
                    self.lag_weight * lag_normalized
            )

            return {
                'time_entropy': time_entropy,
                'weekday_entropy': weekday_entropy,
                'lag_entropy': lag_entropy,
                'time_normalized': time_normalized,
                'weekday_normalized': weekday_normalized,
                'lag_normalized': lag_normalized,
                'weighted_entropy': weighted_entropy,
                'time_gini': time_gini,
                'time_concentration': time_concentration,
                'num_time_buckets': len(time_30min),
                'num_active_days': sum(1 for v in weekday.values() if v > 0),
                'num_lag_values': len(lag)
            }

        def _calculate_entropy(self, distribution: Dict) -> float:
            """Calculate Shannon entropy"""
            if not distribution or len(distribution) == 1:
                return 0.0

            total = sum(distribution.values())
            if total == 0:
                return 0.0

            entropy = 0.0
            for count in distribution.values():
                if count > 0:
                    p = count / total
                    entropy -= p * math.log2(p)

            return entropy

        def _normalize_entropy(self, entropy: float, max_categories: int) -> float:
            """Normalize entropy to [0,1] range"""
            if max_categories <= 1:
                return 0.0

            max_entropy = math.log2(max_categories)
            return entropy / max_entropy if max_entropy > 0 else 0.0

        def _calculate_gini(self, values: List[float]) -> float:
            """Calculate Gini coefficient"""
            if not values:
                return 0.0

            sorted_vals = sorted(values)
            n = len(sorted_vals)
            cumsum = sum(sorted_vals)

            if cumsum == 0:
                return 0.0

            gini = 0
            for i, val in enumerate(sorted_vals):
                gini += (n - i) * val

            return (2 * gini) / (n * cumsum) - 1

        def _calculate_concentration(self, distribution: Dict) -> float:
            """Calculate concentration ratio"""
            if not distribution:
                return 0.0

            total = sum(distribution.values())
            if total == 0:
                return 0.0

            max_count = max(distribution.values())
            return max_count / total

        def _tiebreaker_by_concentration(self,
                                         time_dist: Dict[str, Dict],
                                         timezones: List[str]) -> Optional[str]:
            """Break ties using concentration metric"""
            concentrations = {}

            for tz in timezones:
                dist = time_dist.get(tz, {})
                if dist:
                    concentration = self._calculate_concentration(dist)
                    concentrations[tz] = concentration

            if not concentrations:
                return None

            return max(concentrations.items(), key=lambda x: x[1])[0]

        def _generate_reason(self,
                             timezone: str,
                             metrics: Dict,
                             all_metrics: Optional[Dict] = None) -> str:
            """Generate detailed reason for entropy-based suggestion"""

            reasons = []

            # Overall entropy assessment
            weighted = metrics['weighted_entropy']
            if weighted < 0.2:
                reasons.append(f"Very low entropy ({weighted:.3f}) indicating high predictability")
            elif weighted < 0.4:
                reasons.append(f"Low entropy ({weighted:.3f}) indicating good predictability")
            elif weighted < 0.6:
                reasons.append(f"Moderate entropy ({weighted:.3f})")
            else:
                reasons.append(f"Higher entropy ({weighted:.3f}) indicating less predictability")

            # Time distribution analysis
            time_entropy = metrics['time_entropy']
            time_buckets = metrics['num_time_buckets']
            reasons.append(f"Time entropy={time_entropy:.3f} bits across {time_buckets} active slots")

            # Concentration analysis
            concentration = metrics['time_concentration']
            if concentration > 0.9:
                reasons.append(f"Extremely concentrated ({concentration:.1%} in peak slot)")
            elif concentration > 0.7:
                reasons.append(f"Highly concentrated ({concentration:.1%} in peak slot)")
            elif concentration > 0.5:
                reasons.append(f"Moderately concentrated ({concentration:.1%} in peak slot)")
            else:
                reasons.append(f"Distributed pattern ({concentration:.1%} in peak slot)")

            # Gini coefficient insight
            gini = metrics['time_gini']
            if gini > 0.8:
                reasons.append(f"High inequality (Gini={gini:.3f})")
            elif gini > 0.5:
                reasons.append(f"Moderate inequality (Gini={gini:.3f})")
            else:
                reasons.append(f"Low inequality (Gini={gini:.3f})")

            # Weekday pattern
            active_days = metrics['num_active_days']
            if active_days == 5:
                reasons.append("Weekday-only pattern")
            elif active_days == 7:
                reasons.append("Full week activity")
            else:
                reasons.append(f"Activity on {active_days} days")

            # Lag analysis
            lag_values = metrics['num_lag_values']
            if lag_values == 1:
                reasons.append("Consistent processing lag")
            else:
                reasons.append(f"Variable processing lag ({lag_values} different values)")

            # Comparative analysis
            if all_metrics and len(all_metrics) > 1:
                entropies = [m['weighted_entropy'] for tz, m in all_metrics.items()]
                min_entropy = min(entropies)
                if weighted == min_entropy:
                    reasons.append("Lowest entropy among all timezones")

                # Compare with second best
                sorted_entropies = sorted(entropies)
                if len(sorted_entropies) > 1:
                    second_best = sorted_entropies[1]
                    if second_best - min_entropy > 0.1:
                        reasons.append(f"Significantly better than next best (Δ={second_best - min_entropy:.3f})")

            return ". ".join(reasons)
