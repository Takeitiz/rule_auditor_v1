import re
from typing import List, Dict
from pytz import timezone
from monitoring_platform.sdk.externals.client.file_event_client import FileEventQueryBuilder
from monitoring_platform.sdk.externals.client.file_pattern_client import FilePatternClient
from monitoring_platform.sdk.dependency_injection.injector import auto_inject
from datnguyen.rule_auditor.collector.base import BaseMetricCollector
from datnguyen.rule_auditor.config import DEFAULT_CONFIG

class FileMonitorMetricCollector(BaseMetricCollector):
    """Collects metrics and events for file monitoring rules"""

    def enrich_events(self, events: List[Dict]) -> List[Dict]:
        """Add file-specific metadata to events"""
        for e in events:
            e.created_time = e.created_time.replace(tzinfo=timezone('UTC'))
        return sorted(events, key=lambda x: (x.file_name, x.created_time))

    @auto_inject(DEFAULT_CONFIG)
    def build_query(self, rule, start_date, end_date, file_pattern_client: FilePatternClient):
        qb = FileEventQueryBuilder()
        qb = (qb
            .filter_event_types(["file_created", "file_updated"])
            .filter_created_time_before(end_date)
            .filter_created_time_after(start_date)
        )

        if rule.pattern_id:
            pattern = file_pattern_client.get_file_pattern(rule.pattern_id)
            wildcard_pattern = re.sub(r'\$\{[^}]+\}', '*', pattern.pattern)
            wildcard_pattern += '*'
            qb = (qb
                .filter_pattern_id(rule.pattern_id)
                .filter_file_name(wildcard_pattern)
            )
        else:
            # TODO: do the same like rule.pattern_id:
            qb = qb.filter_fm_id(rule.id)

        return qb
