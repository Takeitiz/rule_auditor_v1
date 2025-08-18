"""
Base interfaces for statistics calculation.
"""

import os
import importlib
import pandas as pd
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datnguyen.rule_auditor.exceptions import StatisticsError
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator

class StatisticsResult(BaseModel):
    """Base model for all statistics"""
    rule_id: int
    rule_type: str
    start_time: datetime
    end_time: datetime
    total_events: int
    calculation_time: datetime = None
    frequency: dict
    count_thresholds: Dict[str, dict] = Field(default_factory=dict)
    count_percentiles: Dict[str, dict] = Field(default_factory=dict)
    count_distribution: Dict[str, dict] = Field(default_factory=dict)
    count_weekday_distribution: Dict[str, dict] = Field(default_factory=dict)
    count_30_min_distribution: Dict[str, dict] = Field(default_factory=dict)
    holiday_metrics: Dict[str, dict] = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def create(**kwargs) -> 'StatisticsResult':
        rule_type = kwargs.get('rule_type')
        if not rule_type:
            raise StatisticsError("Rule type is required to create StatisticsResult", "create")

        if rule_type == "rule::table_service::table_rule_v2" or rule_type == "rule::ats_rule_v2":
            from datnguyen.rule_auditor.statistics.table_service_stat_calculator import TableServiceStatistics
            return TableServiceStatistics(**kwargs)
        elif rule_type == "rule::og_job_rule":
            from datnguyen.rule_auditor.statistics.og_job_stat_calculator import OGJobStatistics
            return OGJobStatistics(**kwargs)
        elif rule_type == "rule::file_monitor::file_rule_v2":
            from datnguyen.rule_auditor.statistics.file_monitor_stat_calculator import FileMonitorStatistics
            return FileMonitorStatistics(**kwargs)
        else:
            return StatisticsResult(**kwargs)


class StatisticsResultCalculator():
    """Base class for statistics calculators. Acts as a pluggable orchestrator."""
    return_class: StatisticsResult = StatisticsResult

    def __init__(self, timezone: Optional[str] = None):
        self.rule = None
        self.user_timezone = timezone
        self.common_metric_calculators = self._load_common_metric_calculators()

    def _load_common_metric_calculators(self) -> List[BaseMetricCalculator]:
        """Dynamically load all common metric calculators."""
        calculators = []
        metrics_path = os.path.join(os.path.dirname(__file__), 'metrics', 'common')
        for filename in os.listdir(metrics_path):
            if filename.endswith('.py') and filename != 'base.py':
                module_name = f"datnguyen.rule_auditor.statistics.metrics.common.{filename[:-3]}"
                module = importlib.import_module(module_name)
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if isinstance(attribute, type) and issubclass(attribute,
                                                                  BaseMetricCalculator) and attribute is not BaseMetricCalculator:
                        calculators.append(attribute())
        return calculators

    def calculate(self, df: pd.DataFrame) -> Dict:
        """Calculate all common statistics by running pluggable calculators."""
        df = self._prepare_data(df)
        stats_data = {}
        for calculator in self.common_metric_calculators:
            stats_data.update(calculator.calculate(df))
        return stats_data

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._add_custom_features(df)

    def _get_required_columns(self) -> List[str]:
        """Get required columns for statistics calculation"""
        return ['create_time']

    def _add_custom_features(self, df: pd.DataFrame) -> pd.DataFrame:
        return df
