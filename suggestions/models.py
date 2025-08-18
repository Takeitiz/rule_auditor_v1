from types import NoneType
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field
import re

from datnguyen.rule_auditor.suggestions.timezone.models import TimezoneResult
from datnguyen.rule_auditor.suggestions.check_windows.models import CheckWindowsResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_size import FileSizeResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_count import FileCountResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_age import FileAgeResult
from datnguyen.rule_auditor.suggestions.file_monitor.models.file_ownership import FileOwnershipResult


class FileSuggestions(BaseModel):
    """Collection of all file monitoring suggestions"""
    rule_id: int = Field(..., description="ID of the rule these suggestions are for")
    generated_at: datetime = Field(default_factory=datetime.now, description="When the suggestions were generated")
    timezone: Optional[TimezoneResult] = Field(None, description="Timezone suggestion (legacy)")
    check_windows: Optional[CheckWindowsResult] = Field(None, description="Check windows suggestions (new)")
    file_size: Optional[FileSizeResult] = Field(None, description="File size threshold suggestions")
    file_count: Optional[FileCountResult] = Field(None, description="File count threshold suggestions")
    file_age: Optional[FileAgeResult] = Field(None, description="File age threshold suggestions")
    file_ownership: Optional[FileOwnershipResult] = Field(None, description="File ownership and permission suggestions")

    def to_dict(self) -> dict:
        """Convert all suggestions to dictionary format"""
        return {
            'rule_id': self.rule_id,
            'generated_at': self.generated_at.isoformat(),
            'suggestions': {
                'timezone': self.timezone.model_dump() if self.timezone else None,
                'check_windows': self.check_windows.model_dump() if self.check_windows else None,
                'file_size': self.file_size.model_dump() if self.file_size else None,
                'file_count': self.file_count.model_dump() if self.file_count else None,
                'file_age': self.file_age.model_dump() if self.file_age else None,
                'file_ownership': self.file_ownership.model_dump() if self.file_ownership else None
            }
        }

    def apply_to_rule(self, rule) -> None:
        """Apply all suggestions to a rule"""
        # Clear existing constraints
        rule.constraints = []

        # Apply check windows suggestion (new) or timezone suggestion (legacy)
        if self.check_windows:
            self.check_windows.apply_to_rule(rule)
        if self.timezone:
            from monitoring_platform.sdk.model.timezone import Timezone
            rule.timezone = Timezone.get_by_name(self.timezone.timezone)
            if self.timezone.delay_code and self.timezone.delay_value is not None:
                rule.delay_code = self.timezone.delay_code
                rule.delay_value = self.timezone.delay_value

                # Modify the pattern based on delay_code and delay_value
                if rule.pattern:
                    def modify_macro(macro: str) -> str:
                        # Extract the part after the last underscore, e.g., "YYYY" from "B1_YYYY"
                        macro_suffix = macro.split('_')[-1]
                        macro = NoneType()
                        if self.timezone.delay_code == "T":
                            macro = "T"
                        else:
                            macro = self.timezone.delay_code + str(int(self.timezone.delay_value))
                        return f"${{{macro}_{macro_suffix}}}"

                    rule.pattern = re.sub(
                        r"\$\{(.*?(_.*)?\)}",
                        lambda match: modify_macro(match.group(1)),
                        rule.pattern
                    )

        # Apply file size suggestion
        if self.file_size:
            from monitoring_platform.sdk.constraints.file_size_threshold_constraint import FileSizeThresholdConstraint
            constraint = FileSizeThresholdConstraint(
                min_value=self.file_size.min_size,
                max_value=self.file_size.max_size
            )
            rule.add_constraint(constraint)

        # Apply file count suggestion
        if self.file_count:
            from monitoring_platform.sdk.constraints.file_count_constraint import FileCountThresholdConstraint
            constraint = FileCountThresholdConstraint(
                min_value=self.file_count.min_count,
                max_value=self.file_count.max_count
            )
            rule.add_constraint(constraint)

        # Apply file age suggestion
        if self.file_age:
            from monitoring_platform.sdk.constraints.file_max_age_constraint import FileMaxAgeConstraint
            constraint = FileMaxAgeConstraint(max_age=self.file_age.max_age)
            rule.add_constraint(constraint)

        # Apply file ownership suggestion
        if self.file_ownership:
            from monitoring_platform.sdk.constraints.file_ownership_and_permission_constraint import \
                FileOwnershipAndPermissionConstraint
            constraint = FileOwnershipAndPermissionConstraint(
                expected_permission=self.file_ownership.expected_permission,
                expected_owner=self.file_ownership.expected_owner,
                expected_group=self.file_ownership.expected_group
            )
            rule.add_constraint(constraint)

    def to_rule(self, original_rule) -> Any:
        """Create a new rule with suggestions applied"""
        # Import here to avoid circular imports
        from copy import deepcopy

        # Create a deep copy of the original rule
        new_rule = deepcopy(original_rule)

        # Apply all suggestions to the new rule
        self.apply_to_rule(new_rule)

        return new_rule

    class Config:
        arbitrary_types_allowed = True


class TableServiceSuggestions(BaseModel):
    """Collection of all table service monitoring suggestions"""
    rule_id: int = Field(..., description="ID of the rule these suggestions are for")
    generated_at: datetime = Field(default_factory=datetime.now, description="When the suggestions were generated")

    # TODO: Add table service specific fields
    # Example fields might include:
    # - query_timeout: Optional[int]
    # - batch_size: Optional[int]
    # - retry_count: Optional[int]

    def to_dict(self) -> dict:
        """Convert all suggestions to dictionary format"""
        return {
            'rule_id': self.rule_id,
            'generated_at': self.generated_at.isoformat(),
            'suggestions': {}  # TODO: Add table service specific suggestions
        }

    def apply_to_rule(self, rule) -> None:
        """Apply all suggestions to a rule"""
        # TODO: Implement table service specific rule updates
        pass

    class Config:
        arbitrary_types_allowed = True


class OGJobSuggestions(BaseModel):
    """Collection of all OpenGraph job monitoring suggestions"""
    rule_id: int = Field(..., description="ID of the rule these suggestions are for")
    generated_at: datetime = Field(default_factory=datetime.now, description="When the suggestions were generated")

    # TODO: Add OG job specific fields
    # Example fields might include:
    # - job_timeout: Optional[int]
    # - max_retries: Optional[int]
    # - concurrency: Optional[int]

    def to_dict(self) -> dict:
        """Convert all suggestions to dictionary format"""
        return {
            'rule_id': self.rule_id,
            'generated_at': self.generated_at.isoformat(),
            'suggestions': {}  # TODO: Add OG job specific suggestions
        }

    def apply_to_rule(self, rule) -> None:
        """Apply all suggestions to a rule"""
        # TODO: Implement OG job specific rule updates
        pass

    class Config:
        arbitrary_types_allowed = True
