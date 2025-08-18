"""
Statistics calculator for OpenGraph Job monitoring rules.
"""
import os
import importlib
import pandas as pd
from pydantic import Field
from typing import Dict, List, Optional
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from datnguyen.rule_auditor.statistics.models import StatisticsResultCalculator, StatisticsResult


class OGJobStatistics(StatisticsResult):
    """Statistics for OpenGraph job monitoring rules"""
    rule_type: str = "rule::og_job_rule"  # Add rule_type field
    # Job status metrics
    status_counts: Dict[str, int] = Field(default_factory=dict)
    status_transitions: Dict[str, int] = Field(default_factory=dict)

    job_runtimes: Dict[str, float] = Field(default_factory=dict)

    # Runtime metrics
    runtime_thresholds: Dict[str, float] = Field(default_factory=dict)
    runtime_percentiles: Dict[str, float] = Field(default_factory=dict)
    runtime_distribution: Dict[str, float] = Field(default_factory=dict)

    # Error metrics
    error_counts: Dict[str, int] = Field(default_factory=dict)
    retry_metrics: Dict[str, float] = Field(default_factory=dict)

    # Schedule metrics
    schedule_metrics: Dict[str, float] = Field(default_factory=dict)
    time_since_last_job: Optional[float] = None


class OGJobStatisticsCalculator(StatisticsResultCalculator):
    """
    Orchestrator for calculating all OpenGraph job statistics.
    It combines the base calculations with a pluggable system for specific metrics.
    """
    return_class = OGJobStatistics

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.metric_calculator_classes = self._load_metric_calculator_classes()

    def _load_metric_calculator_classes(self) -> List[type]:
        """Dynamically load all metric calculator classes from the 'metrics' directory."""
        calculator_classes = []
        metrics_path = os.path.join(os.path.dirname(__file__), 'metrics', 'og_job')
        for filename in os.listdir(metrics_path):
            if filename.endswith('.py') and filename != 'base.py':
                module_name = f"datnguyen.rule_auditor.statistics.metrics.og_job.{filename[:-3]}"
                module = importlib.import_module(module_name)
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if isinstance(attribute, type) and issubclass(attribute, BaseMetricCalculator) and attribute is not BaseMetricCalculator:
                        calculator_classes.append(attribute)

        return calculator_classes

    def calculate(self, df: pd.DataFrame) -> StatisticsResult:
        """
        Calculate all statistics by combining base calculations with pluggable metrics.
        """
        # Stage 1: Get all common metrics from the base class's pluggable system
        stats_data = super().calculate(df)

        # Stage 2: Instantiate and run all og-job-specific pluggable metric calculators
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
        df['date'] = df['event_time'].dt.date
        return df
