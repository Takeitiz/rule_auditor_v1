from typing import Optional
from datnguyen.rule_auditor.suggestions.base import BaseAlgorithm, requires_metrics
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_ownership import FileOwnershipResult


class FileOwnershipAlgorithm(BaseAlgorithm):
    """File ownership suggestion algorithm using distribution analysis"""

    @requires_metrics('ownership_distribution')
    def suggest(self, statistics: StatisticsResult) -> Optional[FileOwnershipResult]:
        """Generate file ownership suggestion based on statistics"""
        distribution = statistics.ownership_distribution
        if not distribution:
            return None

        # Get individual distributions
        owners = distribution.get('owners', {})
        groups = distribution.get('groups', {})
        permissions = distribution.get('permissions', {})

        if not owners or not groups or not permissions:
            return None

        # Find most common values
        expected_owner = self._get_most_common(owners)
        expected_group = self._get_most_common(groups)
        expected_permission = self._get_most_common(permissions)

        result = FileOwnershipResult(
            expected_permission=expected_permission,
            expected_owner=expected_owner,
            expected_group=expected_group
        )
        result.method_used = "distribution_analysis"
        return result

    def _get_most_common(self, distribution: dict) -> Optional[str]:
        """Get the most common value from a distribution"""
        if not distribution:
            return None
        return max(distribution.items(), key=lambda x: x[1])[0]
