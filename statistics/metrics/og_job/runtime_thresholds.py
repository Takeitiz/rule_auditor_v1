from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class RuntimeThresholdsCalculator(BaseMetricCalculator):
    """Calculates various runtime thresholds."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates min, max, typical, and limit thresholds for runtime.

        Args:
            df: DataFrame with a 'runtime_seconds' column.

        Returns:
            A dictionary containing the runtime thresholds.
        """
        if df.empty or 'runtime_seconds' not in df.columns:
            return {'runtime_thresholds': {}}

        runtimes = df['runtime_seconds'].dropna()
        if runtimes.empty:
            return {'runtime_thresholds': {}}

        thresholds = {
            'min': float(max(0, runtimes.mean() - 2 * runtimes.std())),
            'max': float(runtimes.mean() + 2 * runtimes.std()),
            'typical': float(runtimes.median()),
            'soft_limit': float(runtimes.quantile(0.95)),
            'hard_limit': float(runtimes.quantile(0.99))
        }

        return {'runtime_thresholds': thresholds}
