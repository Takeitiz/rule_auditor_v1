"""
Calculates pattern region counts for file monitoring statistics.
"""
import json
from pathlib import Path
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class PatternRegionCountsCalculator(BaseMetricCalculator):
    """Calculates the count of events per pattern region."""

    def __init__(self, rule):
        super().__init__()
        self.rule = rule
        with open(Path("/home/datnguyen/git/pipeline-operations/python/datnguyen/rule_auditor/pattern_region.json")) as f:
            self.pattern_region_map = json.load(f)

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Looks up the pattern region counts from a predefined map.

        Args:
            df: The input DataFrame (not used directly, but required by the interface).

        Returns:
            A dictionary containing the pattern region counts.
        """
        pattern_id = str(self.rule.pattern_id)
        counts = self.pattern_region_map.get(pattern_id, {})
        return {'pattern_region_counts': counts}
