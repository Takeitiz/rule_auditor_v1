"""
Core DataFrame building functionality
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd
from zoneinfo import ZoneInfo

DEFAULT_TIMEZONES = [
    'America/New_York',
    'Asia/Tokyo',
    'Europe/London',
    'GMT'
]

class BaseDataFrameBuilder(ABC):
    """Base class for building and enriching DataFrames from events."""

    def __init__(self, timezone: Optional[str] = None):
        """The main orchestration method for the builder."""
        self.user_timezone = timezone
        self.timezones_to_analyze = [timezone] if timezone else DEFAULT_TIMEZONES

    def build_events_df(self, events: List[Dict]) -> pd.DataFrame:
        df = self.event_to_df(events)
        if 'timestamp' not in df.columns and df.index.name != 'timestamp':
            raise ValueError("DataFrame must contain 'timestamp' as a column or index.")
        self.add_timezone_features(df)
        self.enrich_df(df)
        mem_usage = df.memory_usage(deep=True).sum()
        print(f"DataFrame memory usage: {mem_usage / 1024 ** 2:.2f} MB")
        return df

    @abstractmethod
    def event_to_df(self, events: List[Dict]) -> pd.DataFrame:
        """Converts a list of event dictionaries to a base DataFrame."""
        raise NotImplementedError

    def enrich_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Hook for subclasses to add specific enrichment steps."""
        return df

    def add_timezone_features(self, df: pd.DataFrame) -> None:
        """Add timezone-based features to the DataFrame."""
        for timezone in self.timezones_to_analyze:
            # For 'timestamp' column
            df[f'timestamp_{timezone}'] = df['timestamp'].dt.tz_convert(timezone)
            df[f'hour_{timezone}'] = df[f'timestamp_{timezone}'].dt.hour
            df[f'30min_bucket_{timezone}'] = df[f'timestamp_{timezone}'].dt.strftime('%H%M').apply(
                lambda x: f"{x[:2]}30" if int(x[2:]) >= 30 else f"{x[:2]}00"
            )
            df[f'day_of_week_{timezone}'] = df[f'timestamp_{timezone}'].dt.dayofweek
            df[f'week_num_{timezone}'] = df[f'timestamp_{timezone}'].dt.isocalendar().week
            df[f'is_dst_{timezone}'] = df[f'timestamp_{timezone}'].apply(
                lambda x: bool(ZoneInfo(timezone).dst(x.replace(tzinfo=None))) if pd.notnull(x) else False
            )

            # For 'mtime' column
            if 'mtime' in df.columns:
                df[f'mtime_{timezone}'] = pd.to_datetime(df['mtime'], unit='ms', utc=True).dt.tz_convert(timezone)
                df[f'mtime_hour_{timezone}'] = df[f'mtime_{timezone}'].dt.hour
                df[f'mtime_30min_bucket_{timezone}'] = df[f'mtime_{timezone}'].dt.strftime('%H%M').apply(
                    lambda x: f"{x[:2]}30" if int(x[2:]) >= 30 else f"{x[:2]}00"
                )
                df[f'mtime_day_of_week_{timezone}'] = df[f'mtime_{timezone}'].dt.dayofweek
                df[f'mtime_week_num_{timezone}'] = df[f'mtime_{timezone}'].dt.isocalendar().week
                df[f'mtime_is_dst_{timezone}'] = df[f'mtime_{timezone}'].apply(
                    lambda x: bool(ZoneInfo(timezone).dst(x.replace(tzinfo=None))) if pd.notnull(x) else False
                )
