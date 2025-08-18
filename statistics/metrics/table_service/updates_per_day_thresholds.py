from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class UpdatesPerDayThresholdsCalculator(BaseMetricCalculator):
    """Calculates updates per day thresholds."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates min, max, and typical updates per day thresholds.

        Args:
            df: DataFrame with a 'date' column.

        Returns:
            A dictionary containing the updates per day thresholds.
        """
        if df.empty or 'date' not in df.columns:
            return {'updates_per_day_thresholds': {}}

        daily_updates = df.groupby('date').size()
        if daily_updates.empty:
            return {'updates_per_day_thresholds': {}}

        mean = daily_updates.mean()
        std = daily_updates.std()

        thresholds = {
            'min': int(max(0, mean - 2 * std)),
            'max': int(mean + 2 * std),
            'typical': int(daily_updates.median())
        }
        return {'updates_per_day_thresholds': thresholds}
