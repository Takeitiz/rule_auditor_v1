# Base classes for suggestion algorithms
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Callable
from functools import wraps
import logging
from pydantic import BaseModel, Field
from datetime import datetime
from datnguyen.rule_auditor.statistics.models import StatisticsResult

logger = logging.getLogger(__name__)


def requires_metrics(*required_metrics: str) -> Callable:
    """
    Decorator to check if required metrics are present in statistics before executing a function.

    Args:
        *required_metrics: Variable number of metric names that must be present in statistics

    Returns:
        Decorated function that validates metrics before execution
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, statistics: StatisticsResult, *args, **kwargs) -> Optional[BaseModel]:
            # Check if all required metrics are present
            missing_metrics = [
                metric for metric in required_metrics
                if not hasattr(statistics, metric)
            ]

            if missing_metrics:
                logger.warning(
                    f"Missing required metrics for {func.__name__}: {missing_metrics}"
                )
                return None

            return func(self, statistics, *args, **kwargs)

        return wrapper

    return decorator


class BaseSuggestion(BaseModel):
    """Base class for all suggestion results"""
    method_used: Optional[str] = Field(None, description="Method used to generate the suggestion")
    generated_at: datetime = Field(default_factory=datetime.now, description="When the suggestion was generated")
    suggest_reason: Optional[str] = Field(None, description="Reason for the suggestion")

    def to_dict(self) -> Dict[str, Any]:
        """Convert suggestion to dictionary format"""
        return self.model_dump()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(method={self.method_used})"


class BaseAlgorithm(ABC):
    """Base class for all suggestion algorithms"""

    @abstractmethod
    def suggest(self, statistics: StatisticsResult) -> Optional[BaseSuggestion]:
        """Generate a suggestion based on statistics.

        Args:
            statistics: Statistics result containing metrics and distributions

        Returns:
            A suggestion object or None if no suggestion could be made
        """
        pass


class BaseSuggestionGenerator(ABC):
    """Base class for all suggestion generators"""

    @abstractmethod
    def generate(self, rule_id: int, statistics: StatisticsResult) -> Any:
        """Generate all suggestions for a rule"""
        pass
