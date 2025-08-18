from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class RuntimeDistributionCalculator(BaseMetricCalculator):
    """Calculates various runtime distribution metrics."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates mean, median, std, skew, and kurtosis for runtime.

        Args:
            df: DataFrame with a 'runtime_seconds' column.

        Returns:
            A dictionary containing the runtime distribution metrics.
        """
        if df.empty or 'runtime_seconds' not in df.columns:
            return {'runtime_distribution': {}}

        runtimes = df['runtime_seconds'].dropna()
        if runtimes.empty:
            return {'runtime_distribution': {}}

        distribution = {
            'mean': float(runtimes.mean()),
            'median': float(runtimes.median()),
            'std': float(runtimes.std()),
            'skew': float(runtimes.skew()),
            'kurtosis': float(runtimes.kurtosis())
        }

        return {'runtime_distribution': distribution}
