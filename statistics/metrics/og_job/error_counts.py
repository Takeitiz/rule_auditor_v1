from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from monitoring_platform.sdk.model.metric.open_graph_job_monitor_metric import OpenGraphJobStatus

class ErrorCountsCalculator(BaseMetricCalculator):
    """Calculates error counts and unique error messages."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates counts of different error messages.

        Args:
            df: DataFrame with 'job_status' and potentially 'error_message' columns.

        Returns:
            A dictionary containing the error counts.
        """
        if df.empty:
            return {'error_counts': {}}

        error_states = [s for s in OpenGraphJobStatus if 'ERROR' in s.value or 'FAILED' in s.value]
        error_df = df[df['job_status'].isin(error_states)]

        error_counts = {}
        if 'error_message' in df.columns:
            error_counts = error_df['error_message'].value_counts().to_dict()

        error_counts['total_errors'] = len(error_df)
        error_counts['unique_errors'] = len(error_counts) - 1  # Subtract the total_errors entry

        return {'error_counts': error_counts}
