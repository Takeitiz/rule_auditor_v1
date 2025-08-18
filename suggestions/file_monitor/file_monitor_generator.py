import os
import importlib
import re
from typing import Optional, Any
from datnguyen.rule_auditor.statistics.models import StatisticsResult
from datnguyen.rule_auditor.suggestions.base import BaseSuggestionGenerator, BaseAlgorithm
from datnguyen.rule_auditor.suggestions.models import FileSuggestions
from datnguyen.rule_auditor.suggestions.timezone.algorithm import TimezoneSuggestionAlgorithm
from datnguyen.rule_auditor.suggestions.check_windows.algorithm import CheckWindowsSuggestionAlgorithm


class FileSuggestionGenerator(BaseSuggestionGenerator):
    """Generator that combines all file monitoring suggestion algorithms"""

    def __init__(self, timezone=None):
        self.timezone_algorithm = TimezoneSuggestionAlgorithm(timezone=timezone)
        self.file_monitor_algorithms = self._load_algorithms()

    def _load_algorithms(self) -> dict[str, BaseAlgorithm]:
        """Dynamically load all algorithms from the 'algorithms' directory."""
        algorithms = {}
        algo_path = os.path.dirname(__file__) + '/algorithms'

        def to_snake_case(name: str) -> str:
            """Converts a PascalCase string to snake_case."""
            s1 = re.sub('(.)(([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

        for filename in os.listdir(algo_path):
            if filename.endswith('.py') and filename != '__init__.py':
                module_name = f"datnguyen.rule_auditor.suggestions.file_monitor.algorithms.{filename[:-3]}"
                module = importlib.import_module(module_name)
                for attribute_name in dir(module):
                    attribute = getattr(module, attribute_name)
                    if isinstance(attribute, type) and issubclass(attribute,
                                                                  BaseAlgorithm) and attribute is not BaseAlgorithm:
                        # Infer the suggestion type from the class name, e.g., "FileAgeAlgorithm" -> "file_age"
                        base_name = attribute.__name__.replace("Algorithm", "")
                        suggestion_type = to_snake_case(base_name)
                        algorithms[suggestion_type] = attribute()

        return algorithms

    def generate(self, rule_id: int, statistics: StatisticsResult, attribute: Optional[str] = None) -> Optional[
        FileSuggestions]:
        """Generate suggestions for all or a single attribute"""
        suggestions = FileSuggestions(rule_id=rule_id)

        # Build the map of all available algorithms
        algorithm_map = {
            'timezone': (self.timezone_algorithm, 'timezone'),
            'check_windows': (lambda stats: self._generate_check_windows(stats), 'check_windows'),
        }
        for suggestion_type, algorithm_instance in self.file_monitor_algorithms.items():
            algorithm_map[suggestion_type] = (algorithm_instance, suggestion_type)

        if attribute:
            if attribute not in algorithm_map:
                return None
            algorithm, field_name = algorithm_map[attribute]
            result = algorithm(statistics) if callable(algorithm) else algorithm.suggest(statistics)
            if not result:
                return None
            setattr(suggestions, field_name, result)
            return suggestions

        # Generate all suggestions
        for attr, (algorithm, field_name) in algorithm_map.items():
            result = algorithm(statistics) if callable(algorithm) else algorithm.suggest(statistics)
            if result:
                setattr(suggestions, field_name, result)

        return suggestions

    def _generate_check_windows(self, statistics: StatisticsResult) -> Optional[Any]:
        """Helper to generate check_windows suggestion"""
        timezone_result = self.timezone_algorithm.suggest(statistics)
        if timezone_result and timezone_result.timezone:
            check_windows_algorithm = CheckWindowsSuggestionAlgorithm(timezone=timezone_result.timezone)
            return check_windows_algorithm.suggest(statistics)
        return None
