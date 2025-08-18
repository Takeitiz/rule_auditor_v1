from typing import Dict, Optional
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class TimeSinceLastJobCalculator(BaseMetricCalculator):
    """Calculates the time elapsed since the last job event."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        Calculates the total seconds since the most recent event time.

        Args:
            df: DataFrame with an 'event_time' column.

        Returns:
            A dictionary containing the time since the last job.
        """
        if df.empty or 'event_time' not in df.columns:
            return {'time_since_last_job': None}

        time_since = (pd.Timestamp.now(tz='UTC') - df['event_time'].max()).total_seconds()
        return {'time_since_last_job': time_since}
