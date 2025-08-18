from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class UpdatesPerDayPercentilesCalculator(BaseMetricCalculator):
    """Calculates updates per day percentiles."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates various updates per day percentiles.

        Args:
            df: DataFrame with a 'date' column.

        Returns:
            A dictionary containing the updates per day percentiles.
        """
        if df.empty or 'date' not in df.columns:
            return {'updates_per_day_percentiles': {}}

        daily_updates = df.groupby('date').size()
        if daily_updates.empty:
            return {'updates_per_day_percentiles': {}}

        percentiles = {
            'p25': float(daily_updates.quantile(0.25)),
            'p50': float(daily_updates.quantile(0.50)),
            'p75': float(daily_updates.quantile(0.75)),
            'p90': float(daily_updates.quantile(0.90)),
            'p95': float(daily_updates.quantile(0.95)),
            'p99': float(daily_updates.quantile(0.99))
        }
        return {'updates_per_day_percentiles': percentiles}
