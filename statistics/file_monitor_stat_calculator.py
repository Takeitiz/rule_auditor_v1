import os
import importlib
from typing import Dict, List
from pydantic import Field
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from datnguyen.rule_auditor.statistics.models import StatisticsResultCalculator, StatisticsResult

class FileMonitorStatistics(StatisticsResult):
    """Combined statistics for file count rules"""
    rule_type: str = "rule::file_monitor::file_rule_v2"
    count_date_label_lag_distribution: Dict[str, dict] = Field(default_factory=dict)
    count_mtime_date_label_lag_distribution: Dict[str, dict] = Field(default_factory=dict)
    count_mtime_weekday_distribution: Dict[str, dict] = Field(default_factory=dict)
    count_30_min_dst_distribution: Dict[str, dict] = Field(default_factory=dict)
    count_30_min_non_dst_distribution: Dict[str, dict] = Field(default_factory=dict)
    pattern_region_counts: Dict[str, int] = Field(default_factory=dict)
    anomaly_scores: Dict[str, float] = Field(default_factory=dict)
    size_thresholds: Dict[str, int] = Field(default_factory=dict)
    size_percentiles: Dict[str, float] = Field(default_factory=dict)
    size_distribution: Dict[str, float] = Field(default_factory=dict)
    size_categories: Dict[str, int] = Field(default_factory=dict)
    growth_metrics: Dict[str, float] = Field(default_factory=dict)
    ownership_distribution: Dict[str, Dict[str, int]] = Field(default_factory=dict)


class FileMonitorStatisticsCalculator(StatisticsResultCalculator):
    """
    Orchestrator for calculating all file monitoring statistics.
    It combines the base calculations with a pluggable system for specific metrics.
    """
    return_class = FileMonitorStatistics

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.metric_calculator_classes = self._load_metric_calculator_classes()

    def _load_metric_calculator_classes(self) -> List[type]:
        """Dynamically load all metric calculator classes from the 'metrics' directory."""
        calculator_classes = []
        metrics_path = os.path.join(os.path.dirname(__file__), 'metrics', 'file_monitor')
        for filename in os.listdir(metrics_path):
            if filename.endswith('.py') and filename != 'base.py':
                module_name = f"datnguyen.rule_auditor.statistics.metrics.file_monitor.{filename[:-3]}"
                module = importlib.import_module(module_name)
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if isinstance(attribute, type) and issubclass(attribute,
                                                                  BaseMetricCalculator) and attribute is not BaseMetricCalculator:
                        calculator_classes.append(attribute)
        return calculator_classes

    def calculate(self, df: pd.DataFrame) -> StatisticsResult:
        """
        Calculate all statistics by combining base calculations with pluggable metrics.
        """
        # Stage 1: Get all common metrics from the base class's pluggable system
        stats_data = super().calculate(df)

        # Stage 2: Instantiate and run all file-monitor-specific pluggable metric calculators
        df = self._prepare_data(df)  # Prepare data once
        for calculator_class in self.metric_calculator_classes:
            # Handle special case for PatternRegionCountsCalculator which needs the rule object
            if calculator_class.__name__ == 'PatternRegionCountsCalculator':
                calculator_instance = calculator_class(self.rule)
            else:
                calculator_instance = calculator_class()

            metric_result = calculator_instance.calculate(df)
            stats_data.update(metric_result)

        # Finalize and return the complete statistics object
        stats_data.update({
            "rule_id": self.rule.id,
            "rule_type": self.rule.type,
            "start_time": df['timestamp'].min() if 'timestamp' in df.columns else pd.Timestamp.now(),
            "end_time": df['timestamp'].max() if 'timestamp' in df.columns else pd.Timestamp.now(),
            "total_events": len(df),
            "calculation_time": pd.Timestamp.now()
        })

        return self.return_class(**stats_data)
