"""
Calculates size percentiles for file monitoring statistics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class SizePercentilesCalculator(BaseMetricCalculator):
    """Calculates various size percentiles based on the 'size' column."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates p5, p25, p50, p75, p90, p95, and p99 size percentiles.

        Args:
            df: DataFrame with a 'size' column.

        Returns:
            A dictionary containing the calculated size percentiles.
        """
        if 'size' not in df.columns:
            return {'size_percentiles': {}}

        sizes = df['size'].dropna()
        if sizes.empty:
            return {'size_percentiles': {}}

        percentiles = {
            'p5': float(sizes.quantile(0.05)),
            'p25': float(sizes.quantile(0.25)),
            'p50': float(sizes.quantile(0.50)),
            'p75': float(sizes.quantile(0.75)),
            'p90': float(sizes.quantile(0.90)),
            'p95': float(sizes.quantile(0.95)),
            'p99': float(sizes.quantile(0.99))
        }
        return {'size_percentiles': percentiles}
