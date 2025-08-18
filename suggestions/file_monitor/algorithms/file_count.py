from typing import Optional
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_count import FileCountResult


class FileCountAlgorithm(BaseAlgorithm):
    """File count suggestion algorithm using percentile analysis"""

    @requires_metrics('count_percentiles')
    def suggest(self, statistics: StatisticsResult) -> Optional[FileCountResult]:
        """Generate file count suggestion based on statistics"""
        percentiles = statistics.count_percentiles
        key = list(percentiles.keys())[0]
        p95 = percentiles[key].get('p95', 0)
        p5 = percentiles[key].get('p5', 0)

        min_threshold = max(1, int(p5))  # At least 1 file
        max_threshold = int(p95)  # Use p95 directly for max

        result = FileCountResult(
            min_count=min_threshold,
            max_count=max_threshold
        )
        result.method_used = "percentile_analysis"
        return result
