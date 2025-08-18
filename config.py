from pathlib import Path

"""Centralized configuration for the Rule Auditor application."""

# Default path to the dependency injection configuration file.
DEFAULT_CONFIG = "/home/datnguyen/git/pipeline-operations/python/datnguyen/rule_auditor/config.yaml"

# Default directory for storing analysis results.
DEFAULT_OUTPUT_DIR = Path("/home/datnguyen/git/pipeline-operations/python/datnguyen/rule_auditor/output")

# Default filename for the parallel analysis summary.
DEFAULT_ANALYSIS_FILENAME = "analysis_results.txt"

# Default number of max workers for parallel processing.
DEFAULT_MAX_WORKERS = 16
