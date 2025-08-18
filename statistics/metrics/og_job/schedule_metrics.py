from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class ScheduleMetricsCalculator(BaseMetricCalculator):
    """Calculates various schedule-related metrics."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates mean/std jobs per day and schedule regularity.

        Args:
            df: DataFrame with a 'date' column.

        Returns:
            A dictionary containing the schedule metrics.
        """
        if df.empty or 'date' not in df.columns:
            return {'schedule_metrics': {}}

        daily_jobs = df.groupby('date').size()
        if daily_jobs.empty:
            return {'schedule_metrics': {}}

        metrics = {
            'jobs_per_day_mean': float(daily_jobs.mean()),
            'jobs_per_day_std': float(daily_jobs.std()),
            'schedule_regularity': float(1 / (daily_jobs.std() / daily_jobs.mean()) if daily_jobs.mean() > 0 else 0)
        }
        return {'schedule_metrics': metrics}
