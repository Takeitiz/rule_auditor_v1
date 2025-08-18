"""
Calculates size distribution for file monitoring statistics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class SizeDistributionCalculator(BaseMetricCalculator):
    """Calculates various size distribution metrics based on the 'size' column."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates mean, median, std, skew, kurtosis, and total_size.

        Args:
            df: DataFrame with a 'size' column.

        Returns:
            A dictionary containing the calculated size distribution metrics.
        """
        if 'size' not in df.columns:
            return {'size_distribution': {}}

        sizes = df['size'].dropna()
        if sizes.empty:
            return {'size_distribution': {}}

        distribution = {
            'mean': float(sizes.mean()),
            'median': float(sizes.median()),
            'std': float(sizes.std()),
            'skew': float(sizes.skew()),
            'kurtosis': float(sizes.kurtosis()),
            'total_size': float(sizes.sum())
        }
        return {'size_distribution': distribution}
