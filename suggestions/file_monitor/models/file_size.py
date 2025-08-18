from typing import Optional
from pydantic import Field
from datnguyen.rule_auditor.suggestions.base import BaseSuggestion


class FileSizeResult(BaseSuggestion):
    """Result from file size suggestion algorithm"""
    min_size: Optional[int] = Field(None, description="Minimum file size in bytes")
    max_size: Optional[int] = Field(None, description="Maximum file size in bytes")

    def __str__(self) -> str:
        return (
            f"FileSizeResult(min_size={self._format_size(self.min_size)}, "
            f"max_size={self._format_size(self.max_size)}, "
            f"method={self.method_used})"
        )

    def _format_size(self, size: Optional[int]) -> str:
        """Format size in human-readable format"""
        if size is None:
            return "None"

        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}TB"
