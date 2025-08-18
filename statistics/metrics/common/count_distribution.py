"""
Calculates count distribution.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class CountDistributionCalculator(BaseMetricCalculator):
    """Calculates count distribution for all timezones."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates various count distribution metrics.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the count distribution metrics.
        """
        count_distribution = {}
        for column in df.columns:
            if column.startswith("timestamp_"):
                timezone = column.replace("timestamp_", "")
                daily_counts = df.groupby(df[column].dt.date).size()
                if not daily_counts.empty:
                    count_distribution[timezone] = {
                        'mean': float(daily_counts.mean()),
                        'median': float(daily_counts.median()),
                        'std': float(daily_counts.std()),
                        'skew': float(daily_counts.skew()),
                        'kurtosis': float(daily_counts.kurtosis())
                    }

        return {'count_distribution': count_distribution}
