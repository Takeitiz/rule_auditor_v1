"""
Calculates the frequency metric.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class FrequencyCalculator(BaseMetricCalculator):
    """Calculates the event frequency."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the frequency of events.

        Args:
            df: The input DataFrame.

        Returns:
            A dictionary containing the frequency metrics.
        """
        if 'timestamp' not in df.columns or df.empty:
            return {'frequency': {}}

        unique_dates = df['timestamp'].dt.date.nunique()
        start_date = df['timestamp'].min().date()
        end_date = df['timestamp'].max().date()
        total_events = len(df)

        metrics = {
            "frequency": total_events / unique_dates if unique_dates > 0 else 0.0,
            "total_events": total_events,
            "start_date": start_date,
            "end_date": end_date
        }

        return {'frequency': metrics}
