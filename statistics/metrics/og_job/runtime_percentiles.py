from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class RuntimePercentilesCalculator(BaseMetricCalculator):
    """Calculates various runtime percentiles."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates p25, p50, p75, p90, p95, and p99 runtime percentiles.

        Args:
            df: DataFrame with a 'runtime_seconds' column.

        Returns:
            A dictionary containing the runtime percentiles.
        """
        if df.empty or 'runtime_seconds' not in df.columns:
            return {'runtime_percentiles': {}}

        runtimes = df['runtime_seconds'].dropna()
        if runtimes.empty:
            return {'runtime_percentiles': {}}

        percentiles = {
            'p25': float(runtimes.quantile(0.25)),
            'p50': float(runtimes.quantile(0.50)),
            'p75': float(runtimes.quantile(0.75)),
            'p90': float(runtimes.quantile(0.90)),
            'p95': float(runtimes.quantile(0.95)),
            'p99': float(runtimes.quantile(0.99))
        }

        return {'runtime_percentiles': percentiles}
