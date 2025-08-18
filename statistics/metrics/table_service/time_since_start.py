from typing import Dict, Optional
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class TimeSinceStartCalculator(BaseMetricCalculator):
    """Calculates the time elapsed since the first event."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        """
        Calculates the total seconds since the earliest event time.

        Args:
            df: DataFrame with a 'timestamp' column.

        Returns:
            A dictionary containing the time since the first event.
        """
        if df.empty or 'timestamp' not in df.columns:
            return {'time_since_start': None}

        time_since = (pd.Timestamp.now(tz='UTC') - df['timestamp'].min()).total_seconds()
        return {'time_since_start': time_since}
