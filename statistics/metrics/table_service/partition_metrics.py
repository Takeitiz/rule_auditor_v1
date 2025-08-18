from typing import Dict
import pandas as pd
import numpy as np
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class PartitionMetricsCalculator(BaseMetricCalculator):
    """Calculates various metrics for each partition."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates row counts, rows per day, updates per day, and age for each partition.

        Args:
            df: DataFrame with 'partitionName', 'actual_rows', 'timestamp', etc.

        Returns:
            A dictionary containing the partition metrics.
        """
        if df.empty or 'partitionName' not in df.columns:
            return {'partition_metrics': {}}

        partition_metrics = {}
        daily_updates = df.groupby(df['timestamp'].dt.date)['timestamp'].size().to_dict()
        for partitionName, partition_df in df.groupby('partitionName'):
            if not partitionName:
                continue

            current_rows = partition_df['actual_rows'].max() if 'actual_rows' in partition_df.columns else 0
            time_range = (partition_df['timestamp'].max() - partition_df['timestamp'].min()).total_seconds()
            days = max(time_range / 86400, 1)
            time_since_last_update = partition_df['time_since_last_update'].dropna()
            age = 0 if time_since_last_update.empty else time_since_last_update.mean()

            partition_date = partition_df['timestamp'].dt.date.iloc[0]
            updates_per_day = daily_updates.get(partition_date, 0)

            partition_metrics[partitionName] = {
                'row_count': current_rows,
                'rows_per_update': [],
                'rows_per_day': np.nan_to_num(current_rows) / days if days else 0,
                'updates_per_day': updates_per_day,
                'age': age,
            }

        return {'partition_metrics': partition_metrics}
