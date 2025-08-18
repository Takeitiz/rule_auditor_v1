from abc import ABC, abstractmethod
from typing import Optional
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.suggestions.check_windows.algorithm import suggest_holiday

class TimezoneStrategy(ABC):
    """Base class for timezone suggestion strategies"""

    def suggest(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[TimezoneResult]:
        """Generate a timezone suggestion based on statistics"""
        result = self.suggest_timezone(statistics, timezone)
        self._add_delay_settings(result, statistics)
        return result

    @abstractmethod
    def suggest_timezone(self, statistics: StatisticsResult, timezone: Optional[str] = None) -> Optional[
        TimezoneResult]:
        pass

    def _add_delay_settings(self, result: TimezoneResult, statistics: StatisticsResult) -> None:
        """Add delay code and value to timezone result"""
        if not result or not result.timezone:
            return

        lag_dist = statistics.count_mtime_date_label_lag_distribution.get(result.timezone, {})
        if not lag_dist:
            return

        largest_label = int(max(lag_dist.items(), key=lambda x: x[1])[0])
        holiday_calendar = suggest_holiday(statistics)

        if largest_label == 0:
            delay_code = "T"
            delay_value = 0
        elif largest_label < 0 and holiday_calendar == "weekday":
            delay_code = "b"
            delay_value = largest_label
        elif largest_label < 0 and holiday_calendar == "all_day":
            delay_code = "c"
            delay_value = largest_label
        elif largest_label > 0 and holiday_calendar == "weekday":
            delay_code = "B"
            delay_value = largest_label
        elif largest_label > 0 and holiday_calendar == "all_day":
            delay_code = "C"
            delay_value = largest_label
        elif largest_label > 0:
            delay_code = "B"
            delay_value = largest_label
        elif largest_label < 0:
            delay_code = "b"
            delay_value = largest_label
        else:
            delay_value = largest_label
            if delay_value == 0:
                delay_code = "T"
            else:
                delay_code = "B"

        result.delay_code = delay_code
        result.delay_value = abs(delay_value)
