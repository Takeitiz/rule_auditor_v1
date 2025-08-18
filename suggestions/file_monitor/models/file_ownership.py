from typing import Optional
from pydantic import Field
from datnguyen.rule_auditor.suggestions.base import BaseSuggestion


class FileOwnershipResult(BaseSuggestion):
    """Result from file ownership suggestion algorithm"""
    expected_permission: Optional[str] = Field(None, description="Expected file permissions")
    expected_owner: Optional[str] = Field(None, description="Expected file owner")
    expected_group: Optional[str] = Field(None, description="Expected file group")

    def __str__(self) -> str:
        return (
            f"FileOwnershipResult("
            f"permission={self.expected_permission}, "
            f"owner={self.expected_owner}, "
            f"group={self.expected_group}, "
            f"method={self.method_used})"
        )
