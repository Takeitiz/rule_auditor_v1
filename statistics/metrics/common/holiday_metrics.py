"""
Calculates holiday metrics.
"""
from typing import Dict
import pandas as pd
from datnguyen.rule_auditor.statistics.metrics.base import BaseMetricCalculator
from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine_similarity
from scipy.spatial.distance import hamming

def _jaccard_similarity(series1: pd.Series, series2: pd.Series) -> float:
    intersection = (series1 & series2).sum()
    union = (series1 | series2).sum()
    return intersection / union if union != 0 else 0.0

def _cosine_similarity(series1: pd.Series, series2: pd.Series) -> float:
    return sklearn_cosine_similarity([series1], [series2])[0][0]

def _hamming_similarity(series1: pd.Series, series2: pd.Series) -> float:
    return 1 - hamming(series1, series2)

def _calculate_shift_similarity(series1: pd.Series, series2: pd.Series, max_shift: int = 4) -> Dict[str, Dict[str, float]]:
    shift_metrics = {}
    for shift in range(-max_shift, max_shift + 1):
        if shift == 0:
            s1, s2 = series1, series2
        elif shift < 0:
            s1 = series1[-shift:]
            s2 = series2[:shift]
        else:
            s1 = series1[:-shift]
            s2 = series2[shift:]
        if len(s1) == 0 or len(s2) == 0:
            continue
        metrics = {
            'jaccard': _jaccard_similarity(s1, s2),
            'cosine': _cosine_similarity(s1, s2),
            'hamming': _hamming_similarity(s1, s2),
        }
        metrics['mean_similarity'] = sum(metrics.values()) / len(metrics)
        shift_key = f"shift{'+' if shift > 0 else ' '}{shift}"
        shift_metrics[shift_key] = metrics
    return shift_metrics

def _load_holidays(date_range: pd.DatetimeIndex, holiday_file_path: str = "/dat/globaldata/holiday/holidays.iso") -> Dict[str, set]:
    holidays_by_country = {}
    min_str = date_range[0].strftime('%Y%m%d')
    max_str = date_range[-1].strftime('%Y%m%d')
    try:
        with open(holiday_file_path, "r") as f:
            for line in f:
                if not (line := line.strip()):
                    continue
                if len(parts := line.split('\t')) >= 2:
                    country_code = parts[0]
                    holiday_date = parts[1]
                    if min_str <= holiday_date <= max_str:
                        holidays_by_country.setdefault(country_code, set()).add(holiday_date)
    except Exception as e:
        print(f"Warning: Could not load holidays from {holiday_file_path}: {e}")
        print("Using empty holiday data.")
    return holidays_by_country

class HolidayMetricsCalculator(BaseMetricCalculator):
    """Calculates holiday metrics for all timestamp columns."""

    def calculate(self, df: pd.DataFrame) -> Dict[str, Dict]:
        results = {}
        for column in df.columns:
            if not column.startswith('timestamp_'):
                continue
            timezone = column.replace("timestamp_", "")
            event_dates = pd.to_datetime(df[column]).dt.date
            if event_dates.empty:
                continue
            date_range = pd.date_range(event_dates.min(), event_dates.max())
            event_df = pd.DataFrame(index=date_range)
            event_df['event_pattern'] = 0
            event_df.loc[pd.to_datetime(event_dates), 'event_pattern'] = 1
            results[timezone] = {}
            holidays_by_country = _load_holidays(date_range)
            event_df['all_day'] = 1
            event_df['weekday'] = (event_df.index.dayofweek < 5).astype(int)
            for country, holidays in holidays_by_country.items():
                holidays_list = sorted(list(holidays))
                event_df[f'holiday_{country}'] = ((~event_df.index.isin(pd.to_datetime(holidays_list, errors='coerce'))) &
                                                (event_df.index.dayofweek < 5)).astype(int)
            all_metrics = {}
            for ref_col in [col for col in event_df.columns if col.startswith('holiday_') or col in ['all_day', 'weekday']]:
                shift_metrics = _calculate_shift_similarity(event_df['event_pattern'].values, event_df[ref_col].values)
                for shift_key, metrics in shift_metrics.items():
                    metric_key = f"{ref_col}::{shift_key}"
                    country = ref_col.split('_')[1] if ref_col.startswith('holiday_') else ref_col
                    shift = shift_key.replace('shift', '')
                    all_metrics[metric_key] = {
                        'metrics': metrics,
                        'similarity_score': metrics['mean_similarity'],
                        'country': country,
                        'shift': shift,
                        'timezone': timezone
                    }
            sorted_metrics = dict(sorted(
                all_metrics.items(),
                key=lambda x: x[1]['similarity_score'],
                reverse=True
            )[:3])
            results[timezone] = sorted_metrics
        return {'holiday_metrics': results}

