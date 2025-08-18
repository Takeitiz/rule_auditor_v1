"""
Calculates count weekday distribution.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from monitoring_platform.sdk.model.file_event import FileEventType

class CountWeekdayDistributionCalculator(BaseMetricCalculator):
    """Calculates count distribution across weekdays for all timezones."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the distribution of counts across weekdays.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the weekday distribution.
        """
        weekday_distribution = {}
        for column in df.columns:
            if column.startswith("timestamp_"):
                timezone = column.replace("timestamp_", "")
                df['weekday'] = df[column].dt.weekday
                filtered_df = df[df["event_type"] == FileEventType.FILE_CREATED]
                weekday_counts = filtered_df.groupby('weekday').size()
                weekday_distribution[timezone] = {
                    'monday': float(weekday_counts.get(0, 0)),
                    'tuesday': float(weekday_counts.get(1, 0)),
                    'wednesday': float(weekday_counts.get(2, 0)),
                    'thursday': float(weekday_counts.get(3, 0)),
                    'friday': float(weekday_counts.get(4, 0)),
                    'saturday': float(weekday_counts.get(5, 0)),
                    'sunday': float(weekday_counts.get(6, 0))
                }

        return {'count_weekday_distribution': weekday_distribution}
