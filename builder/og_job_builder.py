# DataFrame builder for OpenGraph Job monitoring rules.
import pandas as pd
from typing import List, Dict

from datnguyen.rule_auditor.builder.base import BaseDataFrameBuilder


class OGJobDataFrameBuilder(BaseDataFrameBuilder):
    """Builds DataFrame from OpenGraph Job events"""

    def event_to_df(self, events: List[Dict]) -> pd.DataFrame:
        """Convert OG job events to DataFrame"""
        if not events:
            return pd.DataFrame()

        # Extract relevant fields from events
        records = []
        for event in events:
            records.append(event.dict())

        # Create DataFrame
        df = pd.DataFrame.from_records(records)

        # Sort by event_time
        df = df.sort_values('event_time')
        df = self._calculate_job_runtimes(df)
        return df

    def _calculate_job_runtimes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate runtime for each job"""
        df['job_status'] = df['job_status'].astype(str)

        # Calculate runtime for each job
        job_runtimes = []
        for job_id in df['job_id'].dropna().unique():
            job_events = df[df['job_id'] == job_id].copy()
            job_events = job_events.sort_values('event_time', ascending=True)

            created_events = job_events[job_events['job_status'].str.contains('CREATED')]
            completed_events = job_events[job_events['job_status'].str.contains('COMPLETED|FAILED')]

            if not created_events.empty and not completed_events.empty:
                created_time = created_events.iloc[0]['event_time']
                completed_time = completed_events.iloc[0]['event_time']
                runtime = (completed_time - created_time).total_seconds()

                job_runtimes.append({
                    'job_id': job_id,
                    'runtime_seconds': runtime,
                    'status': completed_events.iloc[0]['job_status'],
                    'event_date': created_time.date()
                })

        job_runtimes_df = pd.DataFrame(job_runtimes)
        df = pd.merge(df, job_runtimes_df, on='job_id', how='left')
        df["timestamp"] = df["event_time"]
        return df
