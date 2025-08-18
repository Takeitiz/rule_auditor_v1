"""
Statistics calculator for TableService monitoring rules.
"""
import os
import logging
import importlib
import pandas as pd
from pydantic import Field
from typing import Dict, List, Optional
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from datnguyen.rule_auditor.statistics.models import StatisticsResultCalculator, StatisticsResult

logger = logging.getLogger(__name__)


class TableServiceStatistics(StatisticsResult):
    """Statistics for table service monitoring rules"""
    rule_type: str = "rule::table_service::table_rule_v2"

    # Partition metrics
    partition_metrics: Dict[str, Dict] = Field(default_factory=dict)

    # Row count metrics
    row_count_thresholds: Dict[str, int] = Field(default_factory=dict)
    row_count_percentiles: Dict[str, float] = Field(default_factory=dict)
    row_count_distribution: Dict[str, float] = Field(default_factory=dict)

    # Update frequency metrics
    updates_per_day_thresholds: Dict[str, int] = Field(default_factory=dict)
    updates_per_day_percentiles: Dict[str, float] = Field(default_factory=dict)
    updates_per_day_distribution: Dict[str, float] = Field(default_factory=dict)

    # Rows per update metrics
    rows_per_update_thresholds: Dict[str, int] = Field(default_factory=dict)
    rows_per_update_percentiles: Dict[str, float] = Field(default_factory=dict)
    rows_per_update_distribution: Dict[str, float] = Field(default_factory=dict)

    # Age metrics
    partition_age_metrics: Dict[str, float] = Field(default_factory=dict)
    time_since_start: Optional[float] = None


class TableServiceStatisticsCalculator(StatisticsResultCalculator):
    """
    Orchestrator for calculating all Table Service statistics.
    It combines the base calculations with a pluggable system for specific metrics.
    """
    return_class = TableServiceStatistics

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.metric_calculator_classes = self._load_metric_calculator_classes()

    def _load_metric_calculator_classes(self) -> List[type]:
        """Dynamically load all metric calculator classes from the 'metrics' directory."""
        calculator_classes = []
        metrics_path = os.path.join(os.path.dirname(__file__), 'metrics', 'table_service')
        for filename in os.listdir(metrics_path):
            if filename.endswith('.py') and filename != 'base.py':
                module_name = f"datnguyen.rule_auditor.statistics.metrics.table_service.{filename[:-3]}"
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

        # Stage 2: Instantiate and run all table-service-specific pluggable metric calculators
        df = self._prepare_data(df)  # Prepare data once
        for calculator_class in self.metric_calculator_classes:
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

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for analysis"""
        if df.empty:
            return df
        df = df.copy()
        df['date'] = df['timestamp'].dt.date
        return df
