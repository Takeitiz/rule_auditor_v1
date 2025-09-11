"""
Models for timezone suggestions.
"""
from typing import Optional
from pydantic import Field
from datnguyen.rule_auditor.suggestions.base import BaseSuggestion

class TimezoneResult(BaseSuggestion):
    """Result from timezone suggestion algorithm"""
    timezone: Optional[str] = Field(None, description="Suggested timezone name")
    delay_code: Optional[str] = Field(None, description="Delay code (T, B, b, C, c)")
    delay_value: Optional[int] = Field(None, description="Delay value in minutes")
    confidence: Optional[int] = Field(None, description="Suggestion confidence")

    def __str__(self) -> str:
        delay_info = ""
        if self.delay_code and self.delay_value is not None:
            delay_info = f", delay={self.delay_code}:{self.delay_value}"

        # TODO: add reason of suggestion
        return f"TimezoneResult(timezone={self.timezone}{delay_info}, method={self.method_used})"
