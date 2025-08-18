"""
Calculates ownership distribution for file monitoring statistics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class OwnershipDistributionCalculator(BaseMetricCalculator):
    """Calculates the distribution of user, group, and mode."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the value counts for user, group, and mode columns.

        Args:
            df: DataFrame which may contain 'user', 'group', and 'mode' columns.

        Returns:
            A dictionary containing the ownership distribution.
        """
        distributions = {}
        if 'user' in df.columns:
            distributions['user'] = df['user'].value_counts().to_dict()
        if 'group' in df.columns:
            distributions['group'] = df['group'].value_counts().to_dict()
        if 'mode' in df.columns:
            distributions['mode'] = df['mode'].value_counts().to_dict()

        return {'ownership_distribution': distributions}
