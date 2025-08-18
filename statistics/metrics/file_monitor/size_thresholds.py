"""
Calculates size thresholds for file monitoring statistics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class SizeThresholdsCalculator(BaseMetricCalculator):
    """Calculates various size thresholds based on the 'size' column."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates min, max, typical, and recommended_max size thresholds.

        Args:
            df: DataFrame with a 'size' column.

        Returns:
            A dictionary containing the calculated size thresholds.
        """
        if 'size' not in df.columns:
            return {'size_thresholds': {}}

        sizes = df['size'].dropna()
        if sizes.empty:
            return {'size_thresholds': {}}

        thresholds = {
            'min': int(max(0, sizes.mean() - 2 * sizes.std())),
            'max': int(sizes.mean() + 2 * sizes.std()),
            'typical': int(sizes.median()),
            'recommended_max': int(sizes.quantile(0.95))
        }
        return {'size_thresholds': thresholds}
