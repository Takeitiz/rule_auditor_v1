from typing import Optional
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_age import FileAgeResult


class FileAgeAlgorithm(BaseAlgorithm):
    """File age suggestion algorithm using statistical analysis"""

    @requires_metrics('age_distribution')
    def suggest(self, statistics: StatisticsResult) -> Optional[FileAgeResult]:
        """Generate file age suggestion based on statistics"""
        distribution = statistics.age_distribution
        if not distribution:
            return None

        # Calculate suggested max age
        mean = distribution.get('mean', 0)
        p95 = distribution.get('p95', mean * 1.5)
        suggested_max_age = max(3600, int(p95))  # At least 1 hour

        result = FileAgeResult(max_age=suggested_max_age)
        result.method_used = "statistical_analysis"
        return result
