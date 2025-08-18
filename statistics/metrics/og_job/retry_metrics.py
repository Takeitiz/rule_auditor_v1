from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class RetryMetricsCalculator(BaseMetricCalculator):
    """Calculates various retry metrics."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates mean/max retries, retry rate, and retry distribution.

        Args:
            df: DataFrame with a 'retry' column.

        Returns:
            A dictionary containing the retry metrics.
        """
        if df.empty or 'retry' not in df.columns:
            return {'retry_metrics': {}}

        retries = df['retry'].dropna()
        if retries.empty:
            return {'retry_metrics': {}}

        metrics = {
            'mean_retries': float(retries.mean()),
            'max_retries': float(retries.max()),
            'retry_rate': float((retries > 0).mean()),
            'retry_distribution': retries.value_counts().to_dict()
        }

        return {'retry_metrics': metrics}
