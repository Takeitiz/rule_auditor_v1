from typing import Optional
from pydantic import Field
from datnguyen.rule_auditor.suggestions.base import BaseSuggestion


class FileAgeResult(BaseSuggestion):
    """Result from file age suggestion algorithm"""
    max_age: Optional[float] = Field(None, description="Maximum age allowed for files in seconds")

    def __str__(self) -> str:
        return (
            f"FileAgeResult(max_age={self._format_duration(self.max_age)}, "
            f"method={self.method_used})"
        )

    def _format_duration(self, seconds: Optional[float]) -> str:
        """Format duration in human-readable format"""
        if seconds is None:
            return "None"

        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.0f}m"
        elif seconds < 86400:
            return f"{seconds / 3600:.1f}h"
        else:
            return f"{seconds / 86400:.1f}d"
