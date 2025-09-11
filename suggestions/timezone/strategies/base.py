import pandas as pd
import numpy as np
from datetime import datetime, date
from collections import Counter
from abc import ABC, abstractmethod
from typing import Optional, Dict, List
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.suggestions.check_windows.algorithm import suggest_holiday
from monitoring_platform.sdk.utils.datetime_helper import add_cal_day, is_busday

class TimezoneStrategy(ABC):
    """Base class for timezone suggestion strategies"""

    def suggest(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        """Generate a timezone suggestion based on statistics"""
        result = self.suggest_timezone(statistics, timezone)
        self._add_delay_settings(result, statistics)
        return result

    @abstractmethod
    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        pass

    def _add_delay_settings(self, result: TimezoneResult, statistics: StatisticsResult) -> None:
        """Add delay code and value to timezone result using statistical pattern detection"""
        if not result or not result.timezone:
            return

        # Get holiday by holiday analysis
        holiday_result = suggest_holiday(statistics, result.timezone)
        holiday = holiday_result.get('country') if holiday_result else None

        # Calculate lag distributions on-demand with country context
        lag_distributions = self._calculate_lag_distributions(statistics, result.timezone, holiday)
        if not lag_distributions:
            return

        # Apply statistical framework to determine best pattern (B vs C)
        delay_pattern = self._detect_delay_pattern(lag_distributions)

        result.delay_code = delay_pattern['code']
        result.delay_value = delay_pattern['value']
        # result.confidence = delay_pattern.get('confidence', 0.0)

    def _extract_raw_data_from_statistics(self, statistics: StatisticsResult) -> Optional[pd.DataFrame]:
        """
        Extract raw DataFrame from statistics object.
        """
        return getattr(statistics, 'raw_dataframe', None)

    def _calculate_lag_distributions(self, statistics: StatisticsResult, timezone: str, country: str) -> Optional[Dict]:
        """
        Calculate calendar and business lag distributions for the specified timezone.
        This method extracts file dates and timestamps from statistics and calculates lags.
        """
        raw_data = self._extract_raw_data_from_statistics(statistics)
        if raw_data is None or raw_data.empty:
            return None

        return self._calculate_lags_for_timezone(raw_data, timezone, country)

    def _calculate_lags_for_timezone(self, df: pd.DataFrame, timezone: str, country: str) -> Dict:
        """Calculate calendar and business lag distributions for a specific timezone"""
        timestamp_col = f'mtime_{timezone}'

        if timestamp_col not in df.columns or 'date_label' not in df.columns:
            return {}

        cal_lags = []
        biz_lags = []
        file_data = []  # For validation

        for i, (_, row) in enumerate(df.iterrows()):
            try:
                # Handle date_label - it could be datetime object or string
                date_label_value = row['date_label']
                if pd.isna(date_label_value):
                    continue

                if isinstance(date_label_value, (pd.Timestamp, datetime)):
                    # Already a datetime object
                    reference_date = date_label_value.date()
                else:
                    # String format - try to parse
                    date_label_str = str(date_label_value)
                    if len(date_label_str) == 8 and date_label_str.isdigit():
                        # YYYYMMDD format
                        reference_date = datetime.strptime(date_label_str, '%Y%m%d').date()
                    else:
                        # Try other common formats
                        reference_date = pd.to_datetime(date_label_str).date()

                # Get processing timestamp and convert to date
                processing_timestamp = row[timestamp_col]
                if pd.isna(processing_timestamp):
                    continue

                processing_date = processing_timestamp.date()

                # Calculate lags (processing_date - reference_date)
                cal_lag = self._calendar_days_between(reference_date, processing_date)
                biz_lag = self._business_days_between(reference_date, processing_date, country)

                cal_lags.append(cal_lag)
                biz_lags.append(biz_lag)

                # Store for validation
                file_data.append({
                    'reference_date': reference_date,
                    'processing_date': processing_date,
                    'cal_lag': cal_lag,
                    'biz_lag': biz_lag,
                    'referenced_dow': reference_date.weekday()
                })

            except (ValueError, AttributeError):
                # Skip invalid dates/timestamps
                continue

        if not cal_lags or not biz_lags:
            return {}

        return {
            'cal_lags': cal_lags,
            'biz_lags': biz_lags,
            'cal_distribution': dict(Counter(cal_lags)),
            'biz_distribution': dict(Counter(biz_lags)),
            'file_data': file_data
        }

    def _calendar_days_between(self, start_date: date, end_date: date) -> int:
        """Calculate calendar days between two dates (can be negative)"""
        return (end_date - start_date).days

    def _business_days_between(self, start_date: date, end_date: date, country: str) -> int:
        """Calculate business days between two dates (can be negative)"""
        if start_date == end_date:
            return 0

        # Use helper function approach
        if end_date > start_date:
            # Forward direction - count business days
            business_days = 0
            current_date = start_date
            while current_date < end_date:
                current_date = add_cal_day(current_date, 1)
                if is_busday(current_date, country):
                    business_days += 1
            return business_days
        else:
            # Backward direction - count business days (negative)
            business_days = 0
            current_date = start_date
            while current_date > end_date:
                if is_busday(current_date, country):
                    business_days -= 1
                current_date = add_cal_day(current_date, -1)
            return business_days

    def _detect_delay_pattern(self, lag_distributions: Dict) -> Dict:
        """
        Use statistical analysis to determine the best delay pattern (B vs C).
        """
        cal_lags = lag_distributions['cal_lags']
        biz_lags = lag_distributions['biz_lags']
        file_data = lag_distributions.get('file_data', [])

        if not cal_lags or not biz_lags:
            return {'code': 'T', 'value': 0, 'confidence': 0.0}

        # Calculate distribution metrics
        cal_metrics = self._calculate_distribution_metrics(cal_lags)
        biz_metrics = self._calculate_distribution_metrics(biz_lags)

        b_score = 0  # Business day pattern score
        c_score = 0  # Calendar day pattern score

        # Compare entropy (lower = more concentrated)
        if biz_metrics.get('entropy', float('inf')) < cal_metrics.get('entropy', float('inf')):
            b_score += 2
        else:
            c_score += 2

        # Compare coefficient of variation (lower = more consistent)
        if biz_metrics.get('cv', float('inf')) < cal_metrics.get('cv', float('inf')):
            b_score += 2
        else:
            c_score += 2

        # Compare unique value count (fewer = simpler)
        if biz_metrics.get('unique_count', float('inf')) < cal_metrics.get('unique_count', float('inf')):
            b_score += 1
        else:
            c_score += 1

        # Compare mode concentration (higher = more concentrated)
        if biz_metrics.get('mode_concentration', 0) > cal_metrics.get('mode_concentration', 0):
            b_score += 1
        else:
            c_score += 1

        # Determine pattern and confidence
        total_score = b_score + c_score
        if b_score > c_score:
            pattern = 'B'  # Business days
            confidence = b_score / total_score
            most_common_lag = Counter(biz_lags).most_common(1)[0][0]
        else:
            pattern = 'C'  # Calendar days
            confidence = c_score / total_score
            most_common_lag = Counter(cal_lags).most_common(1)[0][0]

        # Determine delay code based on pattern and lag direction
        lag_value = abs(most_common_lag)
        if most_common_lag == 0:
            delay_code = 'T'
        elif pattern == 'B':
            delay_code = 'B' if most_common_lag > 0 else 'b'
        else:  # pattern == 'C'
            delay_code = 'C' if most_common_lag > 0 else 'c'

        return {
            'code': delay_code,
            'value': lag_value,
            'confidence': confidence,
            'pattern': pattern,
            'most_common_lag': most_common_lag,
        }

    def _calculate_distribution_metrics(self, lags: List[int]) -> Dict:
        """Calculate distribution metrics for pattern detection"""
        if not lags:
            return {}

        counter = Counter(lags)
        total = len(lags)

        # Entropy calculation
        entropy = 0.0
        for count in counter.values():
            if count > 0:
                p = count / total
                entropy -= p * np.log2(p)

        # Coefficient of variation
        mean_lag = np.mean(lags)
        cv = np.std(lags) / mean_lag if mean_lag != 0 else float('inf')

        # Mode concentration
        mode_count = max(counter.values()) if counter else 0
        mode_concentration = mode_count / total if total > 0 else 0

        # Interquartile range
        iqr = np.percentile(lags, 75) - np.percentile(lags, 25) if len(lags) > 1 else 0

        return {
            'entropy': entropy,
            'cv': cv,
            'unique_count': len(counter),
            'mode_concentration': mode_concentration,
            'iqr': iqr
        }

