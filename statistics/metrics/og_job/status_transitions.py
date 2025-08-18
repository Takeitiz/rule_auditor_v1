from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class StatusTransitionsCalculator(BaseMetricCalculator):
    """Calculates the counts of status transitions."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the count of transitions from one status to another.

        Args:
            df: DataFrame with 'event_time' and 'job_status' columns.

        Returns:
            A dictionary containing the status transition counts.
        """
        if df.empty or 'event_time' not in df.columns or 'job_status' not in df.columns:
            return {'status_transitions': {}}

        df_sorted = df.sort_values('event_time')
        transitions = {}
        prev_status = None
        for status in df_sorted['job_status']:
            if prev_status is not None:
                transition = f"{prev_status}->{status}"
                transitions[transition] = transitions.get(transition, 0) + 1
            prev_status = status

        return {'status_transitions': transitions}
