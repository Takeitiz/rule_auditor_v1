"""
Calculates count thresholds.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class CountThresholdsCalculator(BaseMetricCalculator):
    """Calculates count thresholds for all timezones."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates min, max, and typical count thresholds.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the count thresholds.
        """
        count_thresholds = {}
        for column in df.columns:
            if column.startswith("timestamp_"):
                timezone = column.replace("timestamp_", "")
                daily_counts = df.groupby(df[column].dt.date).size()
                if not daily_counts.empty:
                    count_thresholds[timezone] = {
                        'min': int(max(1, daily_counts.mean() - 3 * daily_counts.std())),
                        'max': int(daily_counts.mean() + 3 * daily_counts.std()),
                        'typical': int(daily_counts.median())
                    }

        return {'count_thresholds': count_thresholds}
