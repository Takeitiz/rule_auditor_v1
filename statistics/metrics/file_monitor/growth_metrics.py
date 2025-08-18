"""
Calculates growth metrics for file monitoring statistics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class GrowthMetricsCalculator(BaseMetricCalculator):
    """Calculates various growth metrics based on daily size."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates mean/std daily growth, total growth, and volatility.

        Args:
            df: DataFrame with 'timestamp' and 'size' columns.

        Returns:
            A dictionary containing the calculated growth metrics.
        """
        if 'timestamp' not in df.columns or 'size' not in df.columns:
            return {'growth_metrics': {}}

        daily_sizes = df.groupby(df['timestamp'].dt.date)['size'].sum()
        if len(daily_sizes) < 2:
            return {'growth_metrics': {}}

        growth_rate = daily_sizes.pct_change().dropna()

        metrics = {
            'daily_growth_mean': float(growth_rate.mean()),
            'daily_growth_std': float(growth_rate.std()),
            'total_growth': float((daily_sizes.iloc[-1] / daily_sizes.iloc[0] - 1)),
            'growth_volatility': float(growth_rate.std() / abs(growth_rate.mean())) if abs(
                growth_rate.mean()) > 0 else 0.0
        }

        return {'growth_metrics': metrics}
