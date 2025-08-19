"""
Enhanced Timezone Suggestion Algorithm with Ensemble Approach
"""

import numpy as np
from typing import Optional, Dict, List, Tuple
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.statistics.models import StatisticsResult

# Import all strategies
from datnguyen.rule_auditor.suggestions.timezone.strategies.user_defined_strategy import UserDefinedStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.entropy_strategy import EntropyStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.dst_strategy import DSTStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.cache_region_strategy import CacheRegionStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.circular_variance_strategy import CircularVarianceStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.spectral_analysis_strategy import SpectralAnalysisStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.mutual_information_strategy import MutualInformationStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.autocorrelation_strategy import AutocorrelationStrategy
from datnguyen.rule_auditor.suggestions.timezone.strategies.cross_correlation_strategy import CrossCorrelationStrategy


class TimezoneSuggestionAlgorithm(BaseAlgorithm):
    """
    Enhanced timezone suggestion algorithm using ensemble of strategies.
    Combines multiple mathematical approaches with weighted voting.
    """

    def __init__(self, timezone: Optional[str] = None):
        self.timezone = timezone
        self.strategies = self._initialize_strategies()
        self.strategy_weights = self._get_strategy_weights()

    def _initialize_strategies(self) -> List:
        """Initialize all available strategies"""
        return [
            UserDefinedStrategy(self.timezone),
            CircularVarianceStrategy(),
            SpectralAnalysisStrategy(),
            MutualInformationStrategy(),
            AutocorrelationStrategy(),
            CrossCorrelationStrategy(),
            EntropyStrategy(),  # Keep existing
            DSTStrategy(),  # Keep existing
            CacheRegionStrategy()  # Keep existing
        ]

    def _get_strategy_weights(self) -> Dict[str, float]:
        """Define weights for each strategy based on reliability"""
        return {
            'user_defined': 1.0,  # Always trust user input
            'circular_variance': 0.25,  # Most mathematically sound
            'spectral_analysis': 0.20,  # Excellent for periodicity
            'mutual_information': 0.15,  # Good for dependencies
            'autocorrelation': 0.10,  # Fast and reliable
            'cross_correlation': 0.10,  # Good with references
            'entropy': 0.08,  # Original method
            'dst_analysis': 0.07,  # Specific use case
            'cache_region': 0.05  # Initial filtering
        }

    @requires_metrics('count_weekday_distribution', 'count_30_min_distribution')
    def suggest(self, statistics: StatisticsResult) -> Optional[TimezoneResult]:
        """
        Generate timezone suggestion using ensemble voting.
        Combines results from multiple strategies with confidence weighting.
        """

        # Collect suggestions from all strategies
        suggestions = []
        strategy_results = {}

        for strategy in self.strategies:
            try:
                result = strategy.suggest(statistics)
                if result and result.timezone:
                    # Ensure each result has confidence
                    if not hasattr(result, 'confidence'):
                        result.confidence = 0.5  # Default confidence

                    suggestions.append(result)
                    strategy_name = result.method_used or strategy.__class__.__name__
                    strategy_results[strategy_name] = result

            except Exception as e:
                # Log error but continue with other strategies
                print(f"Strategy {strategy.__class__.__name__} failed: {e}")
                continue

        if not suggestions:
            return None

        # Special case: if user defined, return immediately
        if 'user_defined' in strategy_results:
            return strategy_results['user_defined']

        # Use ensemble voting for other strategies
        return self._ensemble_vote(suggestions, strategy_results)

    def _ensemble_vote(self, suggestions: List[TimezoneResult],
                       strategy_results: Dict[str, TimezoneResult]) -> Optional[TimezoneResult]:
        """
        Combine suggestions using weighted voting with confidence scores.
        """

        # Group suggestions by timezone
        timezone_votes = {}
        timezone_details = {}

        for suggestion in suggestions:
            tz = suggestion.timezone
            if tz not in timezone_votes:
                timezone_votes[tz] = 0
                timezone_details[tz] = []

            # Get strategy weight
            strategy_name = suggestion.method_used
            weight = self.strategy_weights.get(strategy_name, 0.05)

            # Combine weight with confidence
            vote_strength = weight * suggestion.confidence
            timezone_votes[tz] += vote_strength

            timezone_details[tz].append({
                'strategy': strategy_name,
                'confidence': suggestion.confidence,
                'weight': weight,
                'vote': vote_strength,
                'reason': getattr(suggestion, 'suggest_reason', '')
            })

        # Find timezone with highest combined score
        if not timezone_votes:
            return None

        best_tz = max(timezone_votes.items(), key=lambda x: x[1])
        best_timezone = best_tz[0]
        best_score = best_tz[1]

        # Create final result
        result = TimezoneResult(timezone=best_timezone)
        result.method_used = "ensemble"
        result.confidence = min(best_score, 1.0)  # Cap at 1.0

        # Add detailed reasoning
        details = timezone_details[best_timezone]
        supporting_strategies = [d['strategy'] for d in details]

        result.suggest_reason = (
            f"Ensemble decision (confidence={result.confidence:.3f}) "
            f"supported by {len(supporting_strategies)} strategies: "
            f"{', '.join(supporting_strategies[:3])}"
        )

        # Store additional details for debugging
        result.ensemble_details = {
            'all_votes': timezone_votes,
            'strategy_details': timezone_details[best_timezone],
            'num_strategies': len(supporting_strategies)
        }

        # Apply minimum confidence threshold
        if result.confidence < 0.3:
            return None

        # Add delay settings if applicable
        self._add_delay_settings(result, statistics)

        return result

    def _add_delay_settings(self, result: TimezoneResult, statistics: StatisticsResult) -> None:
        """Add delay code and value based on date label lag analysis"""

        if not result or not result.timezone:
            return

        # Get lag distribution for the selected timezone
        lag_dist = statistics.count_mtime_date_label_lag_distribution.get(result.timezone, {})
        if not lag_dist:
            return

        # Find the most common lag
        if lag_dist:
            largest_label = max(lag_dist.items(), key=lambda x: x[1])[0]
            largest_label = int(largest_label)

            # Determine holiday pattern
            from datnguyen.rule_auditor.suggestions.check_windows.algorithm import suggest_holiday
            holiday_calendar = suggest_holiday(statistics)

            # Determine delay code based on lag and holiday pattern
            if largest_label == 0:
                delay_code = "T"
                delay_value = 0
            elif largest_label < 0:
                if holiday_calendar == "weekday":
                    delay_code = "b"
                elif holiday_calendar == "all_day":
                    delay_code = "c"
                else:
                    delay_code = "b"
                delay_value = abs(largest_label)
            else:  # largest_label > 0
                if holiday_calendar == "weekday":
                    delay_code = "B"
                elif holiday_calendar == "all_day":
                    delay_code = "C"
                else:
                    delay_code = "B"
                delay_value = largest_label

            result.delay_code = delay_code
            result.delay_value = delay_value