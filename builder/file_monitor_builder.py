from typing import List, Dict
import numpy as np
import pandas as pd

from datnguyen.rule_auditor.builder.base import BaseDataFrameBuilder

class FileMonitorDataFrameBuilder(BaseDataFrameBuilder):
    """Builds DataFrames from file monitoring events"""

    def event_to_df(self, events: List[Dict]) -> pd.DataFrame:
        """Build DataFrame for file events"""
        if not events:
            return pd.DataFrame()

        df = pd.DataFrame([e.dict() for e in events])
        # Convert 'created_time' to datetime and handle timezone
        df['timestamp'] = pd.to_datetime(df['created_time'], utc=True)
        df = df.sort_values('timestamp')
        return df

    def enrich_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._enrich_date_label(df)
        return df

    def _enrich_date_label(self, df: pd.DataFrame) -> pd.DataFrame:
        """Modified to handle user-defined timezone"""
        df['date_label'] = pd.to_datetime(df['date_label'], errors='coerce')

        for timezone in self.timezones_to_analyze:
            # Convert timestamps to the target timezone
            date_label_tz = df['date_label'].dt.tz_localize(None).dt.tz_localize(timezone)

            # Calculate lags using full timezone names in column names
            valid_mask = df[f'timestamp_{timezone}'].notna() & date_label_tz.notna()
            df.loc[valid_mask, f'date_label_lag_{timezone}'] = (
                    df.loc[valid_mask, f'timestamp_{timezone}'] - date_label_tz[valid_mask]
            ).dt.days

            # Handle mtime calculations
            valid_mtime_mask = df[f'mtime_{timezone}'].notna() & date_label_tz.notna()
            df.loc[valid_mtime_mask, f'mtime_date_label_lag_{timezone}'] = (
                    df.loc[valid_mtime_mask, f'mtime_{timezone}'] - date_label_tz[valid_mtime_mask]
            ).dt.days

        return df
