"""
Calculates anomaly scores for file monitoring statistics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class AnomalyScoresCalculator(BaseMetricCalculator):
    """Calculates various anomaly scores based on event counts."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Calculates Z-scores, anomaly rates, and volatility.

        Args:
            df: DataFrame with a 'timestamp' column.

        Returns:
            A dictionary containing the calculated anomaly scores.
        """
        daily_counts = df.groupby(df['timestamp'].dt.date if 'timestamp' in df.columns else df.index.to_series().dt.date).size()
        mean = daily_counts.mean()
        std = daily_counts.std()
        z_scores = (daily_counts - mean) / std if std > 0 else pd.Series(0, index=daily_counts.index)

        return {
            'anomaly_scores': {
                'max_zscore': float(z_scores.abs().max()),
                'anomaly_rate': float((z_scores.abs() > 3).mean()),
                'volatility': float(std / mean) if mean > 0 else 0.0
            }
        }
