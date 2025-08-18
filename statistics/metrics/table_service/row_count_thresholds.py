from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class RowCountThresholdsCalculator(BaseMetricCalculator):
    """Calculates row count thresholds."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates min, max, and typical row count thresholds.

        Args:
            df: DataFrame with 'date' and 'row_count' columns.

        Returns:
            A dictionary containing the row count thresholds.
        """
        if df.empty or 'row_count' not in df.columns or 'date' not in df.columns:
            return {'row_count_thresholds': {}}

        daily_rows = df.groupby('date')['row_count'].max()
        if daily_rows.empty:
            return {'row_count_thresholds': {}}

        thresholds = {
            'min': int(max(0, daily_rows.mean() - 2 * daily_rows.std())),
            'max': int(daily_rows.mean() + 2 * daily_rows.std()),
            'typical': int(daily_rows.median())
        }

        return {'row_count_thresholds': thresholds}
