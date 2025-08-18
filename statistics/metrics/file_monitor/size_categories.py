"""
Calculates size categories for file monitoring statistics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class SizeCategoriesCalculator(BaseMetricCalculator):
    """Calculates the count of files falling into predefined size categories."""

    SIZE_CATEGORIES = {
        'small': (0, 1024),
        'medium': (1024, 1048576),
        'large': (1048576, 1073741824),
        'huge': (1073741824, float('inf'))
    }

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Calculates the number of files in each size category.

        Args:
            df: DataFrame with a 'size' column.

        Returns:
            A dictionary containing the size category counts.
        """
        if 'size' not in df.columns:
            return {'size_categories': {}}

        categories = {}
        for category, (min_size, max_size) in self.SIZE_CATEGORIES.items():
            count = int(df[(df['size'] >= min_size) & (df['size'] < max_size)].shape[0])
            categories[category] = count

        return {'size_categories': categories}
