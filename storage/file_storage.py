# File-based storage backend implementation

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

from datnguyen.rule_auditor.exceptions import StorageError
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.models import (
    FileSuggestions,
    TableServiceSuggestions,
    OGJobSuggestions
)
from datnguyen.rule_auditor.scoring.models import ReliabilityMetrics
from datnguyen.rule_auditor.storage.base import StorageBackend, StorageKey


class FileStorageBackend(StorageBackend):
    """Stores data in JSON files with a structured directory layout"""

    def __init__(self, base_path: Union[str, Path]):
        self.base_path = Path(base_path)
        self._ensure_base_path()

    def store(self, key: StorageKey, data: Union[
        StatisticsResult, FileSuggestions, TableServiceSuggestions, OGJobSuggestions, ReliabilityMetrics]) -> None:
        """Store data in JSON file"""
        try:
            # Create directory structure
            file_path = self._get_file_path(key)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            # Convert data to JSON-serializable format
            json_data = data.model_dump(mode='json')

            with open(file_path, 'w') as f:
                json.dump(json_data, f, indent=2)

        except Exception as e:
            raise StorageError(f"Failed to store data for {key.rule_id}: {str(e)}", "store")

    def retrieve(self, key: StorageKey) -> Optional[
        Union[StatisticsResult, FileSuggestions, TableServiceSuggestions, OGJobSuggestions, ReliabilityMetrics]]:
        """Retrieve data from JSON file"""
        try:
            file_path = self._get_file_path(key)
            if not file_path.exists():
                return None

            with open(file_path, 'r') as f:
                data = json.load(f)

            if key.data_type == 'statistics':
                return StatisticsResult.model_validate(data)
            elif key.data_type == 'suggestions':
                # This logic for suggestions is a bit weak, as it depends on specific keys.
                # A better approach might be to store a 'suggestion_type' field in the JSON.
                if 'file_size' in data.get('suggestions', {}):
                    return FileSuggestions.model_validate(data)
                elif 'query_timeout' in data.get('suggestions', {}):
                    return TableServiceSuggestions.model_validate(data)
                elif 'job_timeout' in data.get('suggestions', {}):
                    return OGJobSuggestions.model_validate(data)
            elif key.data_type in ('reliability_metric_before', 'reliability_metric_after'):
                return ReliabilityMetrics.model_validate(data)

            return None

        except Exception as e:
            raise StorageError(f"Failed to retrieve data for {key.rule_id}: {str(e)}", "retrieve")

    def list_keys(self, rule_id: Optional[str] = None, data_type: Optional[str] = None) -> List[StorageKey]:
        """List available storage keys"""
        try:
            keys = []
            search_path = self.base_path
            if rule_id:
                search_path = search_path / rule_id
                if not search_path.exists():
                    return []

            # Walk through directory structure
            for root, _, files in os.walk(search_path):
                for file in files:
                    if not file.endswith('.json'):
                        continue

                    try:
                        key = self._parse_file_path(Path(root) / file)
                        if data_type and key.data_type != data_type:
                            continue
                        if rule_id and key.rule_id != rule_id:
                            continue
                        keys.append(key)
                    except ValueError:
                        continue  # Skip files that don't match our naming pattern

            return sorted(keys, key=lambda k: k.rule_id)

        except Exception as e:
            raise StorageError(
                f"Failed to list keys: {str(e)}",
                "list"
            )

    def delete(self, key: StorageKey) -> None:
        """Delete data file"""
        try:
            file_path = self._get_file_path(key)
            if file_path.exists():
                file_path.unlink()

            # Remove empty parent directories
            parent = file_path.parent
            while parent != self.base_path:
                if not any(parent.iterdir()):
                    parent.rmdir()
                parent = parent.parent

        except Exception as e:
            raise StorageError(f"Failed to delete data for {key.rule_id}: {str(e)}", "delete")

    def _ensure_base_path(self) -> None:
        """Ensure base storage directory exists"""
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise StorageError(f"Failed to create storage directory: {str(e)}", "initialization")

    def _get_file_path(self, key: StorageKey) -> Path:
        """Get file path for a storage key"""
        return (
                self.base_path /
                str(key.rule_id) /
                f"{key.data_type}.json"
        )

    def _parse_file_path(self, file_path: Path) -> StorageKey:
        """Parse storage key from file path"""
        # Expected format: <base_path>/<rule_id>/<type>.json
        rule_id = file_path.parent.name
        data_type = file_path.stem  # Remove .json extension
        now = datetime.now()
        return StorageKey(rule_id=rule_id, data_type=data_type, start_date=now, end_date=now)
