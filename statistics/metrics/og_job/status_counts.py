from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class StatusCountsCalculator(BaseMetricCalculator):
    """Calculates the value counts for the 'job_status' column."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the count for each job status.

        Args:
            df: DataFrame with a 'job_status' column.

        Returns:
            A dictionary containing the status counts.
        """
        if df.empty or 'job_status' not in df.columns:
            return {'status_counts': {}}

        counts = df['job_status'].value_counts().to_dict()
        return {'status_counts': counts}
