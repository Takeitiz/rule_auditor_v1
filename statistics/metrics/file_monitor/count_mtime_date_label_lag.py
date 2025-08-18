"""
Calculates the mtime date label lag distribution.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from monitoring_platform.sdk.model.file_event import FileEventType

class CountMtimeDateLabelLagCalculator(BaseMetricCalculator):
    """Calculates the distribution of mtime_date_label_lag for created files."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the distribution of mtime_date_label_lag across all timezones.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the calculated distribution.
        """
        lag_counts = {}
        for column in df.columns:
            if column.startswith("mtime_date_label_lag_"):
                short_tz = column.replace("mtime_date_label_lag_", "")
                filtered_df = df[df['event_type'] == FileEventType.FILE_CREATED]
                lag_counts[short_tz] = filtered_df[column].dropna().value_counts().to_dict()

        return {'count_mtime_date_label_lag_distribution': lag_counts}
