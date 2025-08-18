"""
Core workflow components for rule analysis.
"""

from typing import Dict, Type, Optional
from datetime import datetime
from zoneinfo import ZoneInfo
from monitoring_platform.sdk.factory.const import (
    CUSTOM_MONITOR_RULE,
    TABLE_SERVICE_MONITOR_RULE,
    OPEN_GRAPH_JOB_MONITOR_RULE
)

from datnguyen.rule_auditor.collector.base import BaseMetricCollector
from datnguyen.rule_auditor.collector.file_monitor_collector import FileMonitorMetricCollector
from datnguyen.rule_auditor.collector.table_service_collector import TableServiceMetricCollector
from datnguyen.rule_auditor.collector.og_job_collector import OGJobMetricCollector

from datnguyen.rule_auditor.builder.base import BaseDataFrameBuilder
from datnguyen.rule_auditor.builder.file_monitor_builder import FileMonitorDataFrameBuilder
from datnguyen.rule_auditor.builder.table_service_builder import TableServiceDataFrameBuilder
from datnguyen.rule_auditor.builder.og_job_builder import OGJobDataFrameBuilder

from datnguyen.rule_auditor.statistics.models import StatisticsResultCalculator
from datnguyen.rule_auditor.statistics.file_monitor_stat_calculator import FileMonitorStatisticsCalculator
from datnguyen.rule_auditor.statistics.table_service_stat_calculator import TableServiceStatisticsCalculator
from datnguyen.rule_auditor.statistics.og_job_stat_calculator import OGJobStatisticsCalculator

from datnguyen.rule_auditor.suggestions.file_monitor.file_monitor_generator import FileSuggestionGenerator
from datnguyen.rule_auditor.suggestions.table_service_monitor.table_service_generator import TableServiceSuggestionGenerator
from datnguyen.rule_auditor.suggestions.og_job_monitor.og_job_generator import OGJobSuggestionGenerator


class RuleComponentFactory:
    """Factory for creating rule type specific components"""

    RULE_TYPE_MAPPING: Dict[str, Dict[str, Type]] = {
        CUSTOM_MONITOR_RULE: {
            "collector": FileMonitorMetricCollector("file_event_client"),
            "builder": FileMonitorDataFrameBuilder,
            "calculator": FileMonitorStatisticsCalculator,
            "suggestion_generator": FileSuggestionGenerator
        },
        TABLE_SERVICE_MONITOR_RULE: {
            "collector": TableServiceMetricCollector("table_service_event_client"),
            "builder": TableServiceDataFrameBuilder,
            "calculator": TableServiceStatisticsCalculator,
            "suggestion_generator": TableServiceSuggestionGenerator
        },
        OPEN_GRAPH_JOB_MONITOR_RULE: {
            "collector": OGJobMetricCollector("open_graph_job_client"),
            "builder": OGJobDataFrameBuilder,
            "calculator": OGJobStatisticsCalculator,
            "suggestion_generator": OGJobSuggestionGenerator
        }
    }

    @classmethod
    def create_workflow(cls, rule, tz):
        components = cls.RULE_TYPE_MAPPING[rule.type]

        return RuleAnalysisWorkflow(
            collector=components["collector"],
            builder=components["builder"](timezone=tz),
            calculator=components["calculator"](timezone=tz),
            suggestion_generator=components["suggestion_generator"](timezone=tz)
        )

class RuleAnalysisWorkflow:
    """Orchestrates the complete analysis workflow"""

    def __init__(
        self,
        collector: BaseMetricCollector,
        builder: BaseDataFrameBuilder,
        calculator: StatisticsResultCalculator,
        suggestion_generator: Optional[FileSuggestionGenerator] = None,
    ):
        self.collector = collector
        self.builder = builder
        self.calculator = calculator
        self.suggestion_generator = suggestion_generator

    STEP_ORDER = {
        "collector": 1,
        "scorev1": 2,
        "builder": 3,
        "statistic": 4,
        "suggestion": 5,
        "scorev2": 6
    }

    def analyze_rule(
        self,
        rule,
        tz: str,
        start_date: datetime,
        end_date: datetime,
        step: str = 'scorev2',
        attribute: Optional[str] = None
    ) -> Dict:
        """Analyzes a rule and returns results based on specified step"""
        analysis_results = {}
        target_step = self.STEP_ORDER[step]
        start_date_tz = start_date.replace(tzinfo=ZoneInfo("GMT"))
        end_date_tz = end_date.replace(tzinfo=ZoneInfo("GMT"))

        # Step 1: Collect events once
        if target_step >= 1:
            events = self.collector.collect_events(rule, start_date, end_date)
            analysis_results["events"] = events

        # Step 2: Score rule before suggestion
        if target_step >= 2:
            from datnguyen.rule_auditor.scoring.scoring import scoring_rule
            original_score = scoring_rule(rule, events, start_date_tz, end_date_tz)
            analysis_results["original_score"] = original_score

        # Step 3: Build DataFrame
        if target_step >= 3:
            df = self.builder.build_events_df(events)
            analysis_results["df"] = df

        # Step 4: Calculate statistics
        if target_step >= 4:
            self.calculator.rule = rule
            statistics = self.calculator.calculate(df)
            analysis_results["statistics"] = statistics

        # Step 5: Generate suggestions
        if target_step >= 5:
            suggestions = self.suggestion_generator.generate(rule.id, statistics, attribute)
            analysis_results["suggestions"] = suggestions

        # Step 6: Score rule after suggestion
        if target_step >= 6:
            if suggestions:
                from datnguyen.rule_auditor.scoring.scoring import scoring_rule
                suggested_rule = suggestions.to_rule(rule)
                suggested_score = scoring_rule(suggested_rule, events, start_date_tz, end_date_tz)
                analysis_results["suggested_score"] = suggested_score
                analysis_results["score_improvement"] = suggested_score.final_score - original_score.final_score

        return analysis_results
