"""
Calculates 30-minute bucket distribution.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class Count30MinDistributionCalculator(BaseMetricCalculator):
    """Calculates the distribution of 30-minute buckets for all timezones."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the distribution of events in 30-minute buckets.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the 30-minute bucket distribution.
        """
        bucket_distribution = {}
        for column in df.columns:
            if column.startswith("30min_bucket_"):
                timezone = column.replace("30min_bucket_", "")
                bucket_counts = df[column].value_counts(normalize=True).sort_index()
                bucket_distribution[timezone] = {
                    bucket: float(count) for bucket, count in bucket_counts.items()
                }

        return {"count_30_min_distribution": bucket_distribution}
