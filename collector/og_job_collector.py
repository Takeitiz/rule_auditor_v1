"""
Collector for OpenGraph Job monitoring rules.
"""

from datetime import import datetime
from typing import List, Dict
from monitoring_platform.sdk.externals.client.open_graph_job_client import OpenGraphJobQueryBuilder
from datnguyen.rule_auditor.collector.base import BaseMetricCollector

class OGJobMetricCollector(BaseMetricCollector):
    """Collects metrics and events for OpenGraph job monitoring rules"""

    def enrich_events(self, events: List[Dict]) -> List[Dict]:
        """Add job-specific metadata to events"""
        return sorted(events, key=lambda x: (x.urn, x.event_time))

    def build_query(self, rule, start_date: datetime, end_date: datetime):
        """Build query for OG job events"""
        qb = OpenGraphJobQueryBuilder()
        qb = (qb
            .filter_urn(rule.urn)
            .filter_key_action(rule.key_action)
            .filter_event_time_before(end_date)
            .filter_event_time_after(start_date)
        )

        return qb
