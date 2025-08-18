"""
Calculates count percentiles.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class CountPercentilesCalculator(BaseMetricCalculator):
    """Calculates count percentiles for all timezones."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates various count percentiles.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the count percentiles.
        """
        count_percentiles = {}
        for column in df.columns:
            if column.startswith("timestamp_"):
                timezone = column.replace("timestamp_", "")
                daily_counts = df.groupby(df[column].dt.date).size()
                if not daily_counts.empty:
                    count_percentiles[timezone] = {
                        'p5': float(daily_counts.quantile(0.05)),
                        'p25': float(daily_counts.quantile(0.25)),
                        'p50': float(daily_counts.quantile(0.50)),
                        'p75': float(daily_counts.quantile(0.75)),
                        'p90': float(daily_counts.quantile(0.90)),
                        'p95': float(daily_counts.quantile(0.95)),
                        'p99': float(daily_counts.quantile(0.99))
                    }

        return {'count_percentiles': count_percentiles}
