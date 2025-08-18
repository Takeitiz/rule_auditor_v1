from typing import Optional
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_size import FileSizeResult


class FileSizeAlgorithm(BaseAlgorithm):
    """File size suggestion algorithm using percentile analysis"""

    @requires_metrics('size_percentiles')
    def suggest(self, statistics: StatisticsResult) -> Optional[FileSizeResult]:
        """Generate file size suggestion based on statistics"""
        percentiles = statistics.size_percentiles
        p95 = percentiles.get('p95', 0)
        p5 = percentiles.get('p5', 0)

        # Calculate thresholds
        min_threshold = int(max(1, int(p5)) / 2)  # Half of p5, but at least 1
        max_threshold = int(p95) * 2  # Double p95 for some buffer

        result = FileSizeResult(
            min_size=min_threshold,
            max_size=max_threshold
        )
        result.method_used = "percentile_analysis"
        return result
