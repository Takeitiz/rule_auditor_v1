from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class RowCountPercentilesCalculator(BaseMetricCalculator):
    """Calculates row count percentiles."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates various row count percentiles.

        Args:
            df: DataFrame with 'date' and 'row_count' columns.

        Returns:
            A dictionary containing the row count percentiles.
        """
        if df.empty or 'row_count' not in df.columns or 'date' not in df.columns:
            return {'row_count_percentiles': {}}

        daily_rows = df.groupby('date')['row_count'].max()
        if daily_rows.empty:
            return {'row_count_percentiles': {}}

        percentiles = {
            'p25': float(daily_rows.quantile(0.25)),
            'p50': float(daily_rows.quantile(0.50)),
            'p75': float(daily_rows.quantile(0.75)),
            'p90': float(daily_rows.quantile(0.90)),
            'p95': float(daily_rows.quantile(0.95)),
            'p99': float(daily_rows.quantile(0.99))
        }

        return {'row_count_percentiles': percentiles}
