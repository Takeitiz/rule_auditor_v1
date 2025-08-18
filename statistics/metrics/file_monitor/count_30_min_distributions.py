"""
Calculates the 30-minute bucket distributions for DST and non-DST periods.
"""
from typing import Dict, Tuple
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from monitoring_platform.sdk.model.file_event import FileEventType

class Count30MinDistributionsCalculator(BaseMetricCalculator):
    """Calculates separate 30-minute distributions for DST and non-DST periods."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the 30-minute distributions.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing both dst and non-dst distributions.
        """
        dst_dist, non_dst_dist = self._calculate_distributions(df)
        return {
            'count_30_min_dst_distribution': dst_dist,
            'count_30_min_non_dst_distribution': non_dst_dist,
        }

    def _calculate_distributions(self, df: pd.DataFrame) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, int]]]:
        dst_distribution = {}
        non_dst_distribution = {}
        time_columns = [col for col in df.columns if col.startswith("30min_bucket_")]
        for column in time_columns:
            tz = column.replace("30min_bucket_", "")
            is_dst_col = f'is_dst_{tz}'
            mtime_is_dst_col = f'mtime_is_dst_{tz}'
            created_df = df[df['event_type'] == FileEventType.FILE_CREATED].copy()
            if is_dst_col not in created_df.columns or mtime_is_dst_col not in created_df.columns:
                continue
            dst_mask = created_df[is_dst_col] | created_df[mtime_is_dst_col]
            dst_counts = created_df[dst_mask][column].value_counts().to_dict()
            non_dst_counts = created_df[~dst_mask][column].value_counts().to_dict()
            if dst_counts or non_dst_counts:
                dst_distribution[tz] = dst_counts
                non_dst_distribution[tz] = non_dst_counts
        return dst_distribution, non_dst_distribution
