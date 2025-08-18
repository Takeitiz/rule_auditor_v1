from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class UpdatesPerDayDistributionCalculator(BaseMetricCalculator):
    """Calculates updates per day distribution metrics."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates mean, median, std, skew, and kurtosis for updates per day.

        Args:
            df: DataFrame with a 'date' column.

        Returns:
            A dictionary containing the updates per day distribution metrics.
        """
        if df.empty or 'date' not in df.columns:
            return {'updates_per_day_distribution': {}}

        daily_updates = df.groupby('date').size()
        if daily_updates.empty:
            return {'updates_per_day_distribution': {}}

        distribution = {
            'mean': float(daily_updates.mean()),
            'median': float(daily_updates.median()),
            'std': float(daily_updates.std()),
            'skew': float(daily_updates.skew()),
            'kurtosis': float(daily_updates.kurtosis())
        }
        return {'updates_per_day_distribution': distribution}
