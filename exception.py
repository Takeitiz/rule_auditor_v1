class AuditError(Exception):
    """Base exception for all audit-related errors"""
    pass

class DataCollectionError(AuditError):
    """Error during data collection"""
    def __init__(self, message: str, source: str = None):
        self.source = source
        super().__init__(f"Data collection error from {source}: {message}" if source else message)

class DataProcessingError(AuditError):
    """Error during data processing"""
    def __init__(self, message: str, step: str = None):
        self.step = step
        super().__init__(f"Processing error at {step}: {message}" if step else message)

class InvalidRuleError(AuditError):
    """Error when rule configuration is invalid"""
    def __init__(self, rule_id: int, reason: str):
        self.rule_id = rule_id
        self.reason = reason
        super().__init__(f"Invalid rule {rule_id}: {reason}")

class StatisticsError(AuditError):
    """Error during statistics calculation"""
    def __init__(self, message: str, statistic_type: str = None):
        self.statistic_type = statistic_type
        super().__init__(f"Statistics error ({statistic_type}): {message}" if statistic_type else message)

class InsightGenerationError(AuditError):
    """Error during insight generation"""
    def __init__(self, message: str, insight_type: str = None):
        self.insight_type = insight_type
        super().__init__(f"Insight generation error ({insight_type}): {message}" if insight_type else message)

class StorageError(AuditError):
    """Error during data storage or retrieval"""
    def __init__(self, message: str, operation: str = None):
        self.operation = operation
        super().__init__(f"Storage error during {operation}: {message}" if operation else message)

class ValidationError(AuditError):
    """Error during data validation"""
    def __init__(self, message: str, field: str = None):
        self.field = field
        super().__init__(f"Validation error for {field}: {message}" if field else message)

class ConfigurationError(AuditError):
    """Error in configuration"""
    def __init__(self, message: str, config_key: str = None):
        self.config_key = config_key
        super().__init__(f"Configuration error for {config_key}: {message}" if config_key else message)

class TimeoutError(AuditError):
    """Error when operation times out"""
    def __init__(self, operation: str, timeout: int):
        self.operation = operation
        self.timeout = timeout
        super().__init__(f"Operation {operation} timed out after {timeout} seconds")
