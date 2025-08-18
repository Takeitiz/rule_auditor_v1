from typing import Optional
from pydantic import Field
from datnguyen.rule_auditor.suggestions.base import BaseSuggestion


class FileCountResult(BaseSuggestion):
    """Result from file count suggestion algorithm"""
    min_count: Optional[int] = Field(None, description="Minimum number of files expected")
    max_count: Optional[int] = Field(None, description="Maximum number of files expected")

    def __str__(self) -> str:
        return (
            f"FileCountResult(min_count={self.min_count}, "
            f"max_count={self.max_count}, "
            f"method={self.method_used})"
        )
