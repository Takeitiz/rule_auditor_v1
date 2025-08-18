"""
Core data collection functionality for rule analysis.
"""
from datetime import import datetime, timedelta
from typing import List, Dict
import concurrent
from tqdm import tqdm
import time
from monitoring_platform.sdk.dependency_injection.injector import inject_dependencies
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class BaseMetricCollector(ABC):
    """Base class for collecting metrics from different data sources"""

    def __init__(self, client_name: str):
        self.client_name = client_name
        with inject_dependencies() as container:
            self.event_client = container.get(self.client_name)

    @abstractmethod
    def enrich_events(self, events: List[Dict]) -> List[Dict]:
        """Add rule-specific metadata to events"""
        pass

    @abstractmethod
    def build_query(self, rule, start_date: datetime, end_date: datetime) -> List[Dict]:
        pass

    def collect_events(self, rule, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch all events for a rule, using parallel fetching if necessary."""
        qb = self.build_query(rule, start_date, end_date)
        count = self.event_client.get_count(qb)
        if count >= 2000:
            events = self.get_all_events_parallel(rule, start_date, end_date)
        else:
            events = list(self.event_client.get_all(qb.limit(2000)))
        return self.enrich_events(events)

    def check_if_can_search_now(self):
        """Check if it's safe to make a new search request by monitoring ES open search contexts."""
        if "InMem" in self.event_client.repository.__class__.__name__:
            return True
        while True:
            stats = self.event_client.repository.es_client.nodes.stats()
            total_open_contexts = sum(
                node_stats['indices']['search']['open_contexts'] for node_stats in stats['nodes'].values())
            logger.info(f"Total open search contexts: {total_open_contexts}")
            if total_open_contexts >= 800:
                time.sleep(10)
            else:
                return True

    def get_events_for_date_range(self, rule, start_date, end_date, client):
        # Check if we can safely make a new search request
        self.check_if_can_search_now()
        # Build the query
        qb = self.build_query(rule, start_date, end_date)
        # Fetch and process the events
        qb = qb.limit(10000)
        events = []
        iterator = client.get_all(qb)
        try:
            for event in iterator:
                events.append(event)
        finally:
            iterator.close()
        return events

    def get_all_events_parallel(self, rule, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch events in parallel for large date ranges."""
        date_ranges = []
        start_date = start_date - timedelta(days=7)
        end_date = end_date + timedelta(days=7)
        while start_date < end_date:
            date_ranges.append((start_date, min(start_date + timedelta(days=90), end_date)))
            start_date += timedelta(days=90)

        all_events = []

        def get_events_with_new_client(rule, start_date, end_date):
            # Create a new client instance for each thread
            with inject_dependencies() as container:
                thread_file_event_client = container.get(self.client_name)
                return self.get_events_for_date_range(rule, start_date, end_date, thread_file_event_client)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(get_events_with_new_client, rule, start, end) for (start, end) in date_ranges}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(date_ranges),
                               desc="Fetching events"):
                all_events.extend(future.result())
        return all_events
