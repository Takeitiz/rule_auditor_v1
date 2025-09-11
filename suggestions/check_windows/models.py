"""
Models for check windows suggestions.
"""
from typing import Optional
from pydantic import Field
from datnguyen.rule_auditor.suggestions.base import BaseSuggestion

class CheckWindowsResult(BaseSuggestion):
    """Result from check windows suggestion algorithm"""

    timezone: str = Field(..., description="Timezone for the check windows")
    start_time: Optional[int] = Field(None, description="Start time in seconds from midnight")
    end_time: Optional[int] = Field(None, description="End time in seconds from midnight")
    weekdays: Optional[str] = Field(None, description="Comma-separated list of weekdays")
    holiday_calendar: Optional[str] = Field(None, description="Holiday calendar name")
    day_offset: Optional[int] = Field(None, description="Holiday offset")

    def __str__(self) -> str:
        parts = [f"timezone={self.timezone}"]

        if self.start_time is not None and self.end_time is not None:
            parts.append(f"time={self._format_time(self.start_time)}-{self._format_time(self.end_time)}")

        if self.weekdays:
            parts.append(f"weekdays={self.weekdays}")

        parts.append(f"holidays={self.holiday_calendar}")

        parts.append(f"method={self.method_used}")

        return f"CheckWindowsResult({', '.join(parts)})"

    def _format_time(self, seconds: int) -> str:
        """Format time in HH:MM format"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours:02d}:{minutes:02d}"

    def apply_to_rule(self, rule) -> None:
        """Apply check windows settings to a rule"""
        from monitoring_platform.sdk.model.timezone import Timezone
        from monitoring_platform.sdk.model.check_window_config.weekday_window import WeekdayWindow
        from monitoring_platform.sdk.model.check_window_config.holiday_window import HolidayWindow
        from monitoring_platform.sdk.model.check_window_config.time_window import TimeWindow
        import logging

        logger = logging.getLogger(__name__)

        # Set timezone
        rule.timezone = Timezone.get_by_name(self.timezone)

        # Initialize window lists if needed
        # if not hasattr(rule, 'window_include'):
        rule.window_include = []
        # if not hasattr(rule, 'window_exclude'):
        rule.window_exclude = []

        # Add weekday window
        if self.weekdays:
            # Convert weekday names to numbers (0=Sunday, 6=Saturday)
            weekday_map = {
                'sunday': '0',
                'monday': '1',
                'tuesday': '2',
                'wednesday': '3',
                'thursday': '4',
                'friday': '5',
                'saturday': '6'
            }
            weekday_numbers = []
            for day in self.weekdays.split(','):
                day = day.lower().strip()
                if day in weekday_map:
                    weekday_numbers.append(weekday_map[day])  # Use string numbers
            weekdays_str = ''.join(weekday_numbers)  # Join into comma-separated string
            logger.debug(f"Converting weekdays: {self.weekdays} to numbers: {weekdays_str}")
            window_config = WeekdayWindow(weekdays=weekdays_str)  # Pass string
            rule.window_include.append(window_config)

        # Add time window
        if self.start_time is not None and self.end_time is not None:
            window_config = TimeWindow(start_time=self.start_time, end_time=self.end_time)
            rule.window_include.append(window_config)
            rule.start_time = self.start_time
            rule.end_time = self.end_time

        # Add holiday window if specified
        if self.holiday_calendar:
            if self.holiday_calendar in ("weekday", "all_day"):
                self.holiday_calendar = None
            if self.holiday_calendar:
                window_config = HolidayWindow(holiday_calendar=self.holiday_calendar, day_offset=self.day_offset)
                rule.window_exclude.append(window_config)

