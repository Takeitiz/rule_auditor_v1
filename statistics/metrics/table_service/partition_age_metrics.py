from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class PartitionAgeMetricsCalculator(BaseMetricCalculator):
    """Calculates the age of each partition."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the age of each partition based on the last event time.

        Args:
            df: DataFrame with 'partitionName' and 'timestamp' columns.

        Returns:
            A dictionary containing the age for each partition.
        """
        if df.empty or 'partitionName' not in df.columns or 'timestamp' not in df.columns:
            return {'partition_age_metrics': {}}

        now = pd.Timestamp.now(tz='UTC')
        partition_ages = {}
        for partitionName, partition_df in df.groupby('partitionName'):
            if not partitionName:
                continue
            age = (now - partition_df['timestamp'].max()).total_seconds()
            partition_ages[partitionName] = age

        return {'partition_age_metrics': partition_ages}
