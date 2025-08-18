"""
Calculates the mtime weekday distribution.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from monitoring_platform.sdk.model.file_event import FileEventType

class CountMtimeWeekdayCalculator(BaseMetricCalculator):
    """Calculates the distribution of mtime weekdays for created files."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the distribution of mtime weekdays across all timezones.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the calculated distribution.
        """
        weekday_distribution = {}
        for column in df.columns:
            if column.startswith("mtime_day_of_week_"):
                tz = column.replace("mtime_day_of_week_", "")
                weekday_counts = df[df["event_type"] == FileEventType.FILE_CREATED].groupby(
                    f'mtime_day_of_week_{tz}').size()
                weekday_distribution[tz] = {
                    'monday': float(weekday_counts.get(0, 0)),
                    'tuesday': float(weekday_counts.get(1, 0)),
                    'wednesday': float(weekday_counts.get(2, 0)),
                    'thursday': float(weekday_counts.get(3, 0)),
                    'friday': float(weekday_counts.get(4, 0)),
                    'saturday': float(weekday_counts.get(5, 0)),
                    'sunday': float(weekday_counts.get(6, 0))
                }

        return {'count_mtime_weekday_distribution': weekday_distribution}
