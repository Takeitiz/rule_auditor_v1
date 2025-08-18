"""
DataFrame builder for TableService monitoring rules.
"""

import pandas as pd
from typing import List, Dict

from datnguyen.rule_auditor.builder.base import BaseDataFrameBuilder

class TableServiceDataFrameBuilder(BaseDataFrameBuilder):
    """Builds DataFrame from TableService events"""

    def event_to_df(self, events: List[Dict]) -> pd.DataFrame:
        """Convert TableService events to DataFrame"""
        if not events:
            return pd.DataFrame()

        # Extract relevant fields from events
        records = []
        for event in events:
            records.append(event.dict())

        # Create DataFrame
        df = pd.DataFrame.from_records(records)

        # Calculate actual_rows
        df['actual_rows'] = df['rowNumbers'] - df['startOffset']

        # Sort by timestamp
        df = df.sort_values('timestamp')

        # Calculate time since last update within each partition
        df['time_since_last_update'] = df.groupby('partitionName')['timestamp'].diff().dt.total_seconds()

        return df
