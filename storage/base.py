"""
Base interfaces for data persistence.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel
from datnguyen.rule_auditor.exceptions import StorageError


class StorageKey(BaseModel):
    """Storage key for identifying stored data"""
    rule_id: int
    data_type: str  # 'statistics' or 'suggestions'
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    def to_path(self) -> str:
        """Convert key to storage path"""
        return f"{self.rule_id}/{self.data_type}"


class StorageBackend(ABC):
    """Base class for storage backends"""

    @abstractmethod
    def store(
            self,
            key: StorageKey,
            data: Union['StatisticsResult', List['Suggestion']]
    ) -> None:
        """Store data with the given key"""
        pass

    @abstractmethod
    def retrieve(
            self,
            key: StorageKey
    ) -> Optional[Union['StatisticsResult', List['Suggestion']]]:
        """Retrieve data for the given key"""
        pass

    @abstractmethod
    def list_keys(
            self,
            rule_id: Optional[str] = None,
            data_type: Optional[str] = None
    ) -> List[StorageKey]:
        """List available storage keys"""
        pass

    @abstractmethod
    def delete(self, key: StorageKey) -> None:
        """Delete data for the given key"""
        pass


class StorageManager:
    """Manages data persistence operations"""

    def __init__(self, backend: StorageBackend):
        self.backend = backend

    def store_statistics(
            self,
            rule_id: int,
            statistics: 'StatisticsResult'
    ) -> None:
        """Store statistics for a rule"""
        key = StorageKey(
            rule_id=rule_id,
            start_date=statistics.start_time,
            end_date=statistics.end_time,
            data_type='statistics'
        )
        try:
            self.backend.store(key, statistics)
        except Exception as e:
            raise StorageError(f"Failed to store statistics: {str(e)}", "store")

    def store_suggestions(
            self,
            rule_id: int,
            suggestions: List['Suggestion'],
            start_date: datetime,
            end_date: datetime
    ) -> None:
        """Store suggestions for a rule"""
        key = StorageKey(
            rule_id=rule_id,
            start_date=start_date,
            end_date=end_date,
            data_type='suggestions'
        )
        try:
            self.backend.store(key, suggestions)
        except Exception as e:
            raise StorageError(f"Failed to store suggestions: {str(e)}", "store")

    def list_available_data(
            self,
            rule_id: Optional[str] = None,
            data_type: Optional[str] = None
    ) -> Dict[str, List[StorageKey]]:
        """List available data by rule ID"""
        keys = self.backend.list_keys(rule_id, data_type)
        result = {}
        for key in keys:
            if key.rule_id not in result:
                result[key.rule_id] = []
            result[key.rule_id].append(key)
        return result

    def get_statistics(self, rule_id: int) -> Optional['StatisticsResult']:
        """Get latest statistics for a rule"""
        key = StorageKey(rule_id=rule_id, data_type='statistics')
        return self.backend.retrieve(key)

    def get_suggestions(self, rule_id: int) -> Optional['MonitorSuggestions']:
        """Get latest suggestions for a rule"""
        key = StorageKey(rule_id=rule_id, data_type='suggestions')
        return self.backend.retrieve(key)
