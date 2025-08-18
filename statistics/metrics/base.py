from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd

class BaseMetricCalculator(ABC):
    """Abstract base class for a single metric calculation."""

    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate a specific metric or group of related metrics.

        Args:
            df: The input DataFrame containing the event data.

        Returns:
            A dictionary where keys are the metric names (matching
            the fields in the StatisticsResult model) and values are
            the calculated metrics.
        """
        pass
