"""
Collector for TableService monitoring rules.
"""

from datetime import import datetime
from typing import List, Dict
from monitoring_platform.sdk.externals.client.table_service_event_client import TableServiceEventQueryBuilder
from datnguyen.rule_auditor.collector.base import BaseMetricCollector

class TableServiceMetricCollector(BaseMetricCollector):
    """Collects metrics and events for table service monitoring rules"""

    def enrich_events(self, events: List[Dict]) -> List[Dict]:
        """Add table-specific metadata to events"""
        return sorted(events, key=lambda x: (x.payload.tableName, x.payload.timestamp))

    def build_query(self, rule, start_date: datetime, end_date: datetime):
        """Build query for table service events"""
        qb = TableServiceEventQueryBuilder()
        qb = (qb
            .filter_table_name(rule.table_name)
            .filter_timestamp_before(end_date)
            .filter_timestamp_after(start_date)
        )

        return qb
