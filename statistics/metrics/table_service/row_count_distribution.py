from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator


class RowCountDistributionCalculator(BaseMetricCalculator):
    """Calculates row count distribution metrics."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates mean, median, std, skew, and kurtosis for row count.

        Args:
            df: DataFrame with 'date' and 'row_count' columns.

        Returns:
            A dictionary containing the row count distribution metrics.
        """
        if df.empty or 'row_count' not in df.columns or 'date' not in df.columns:
            return {'row_count_distribution': {}}

        daily_rows = df.groupby('date')['row_count'].max()
        if daily_rows.empty:
            return {'row_count_distribution': {}}

        distribution = {
            'mean': float(daily_rows.mean()),
            'median': float(daily_rows.median()),
            'std': float(daily_rows.std()),
            'skew': float(daily_rows.skew()),
            'kurtosis': float(daily_rows.kurtosis())
        }

        return {'row_count_distribution': distribution}
