from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

class EventDetail(BaseModel):
    """Details about a single event"""
    file_name: str
    timestamp: datetime
    is_covered: bool
    is_holiday: bool
    reason: str = ""  # Reason for not being covered, if any

class AlertDetail(BaseModel):
    """Details about a single alert"""
    resource: str
    severity: str
    open_time: datetime
    close_time: Optional[datetime] = None
    duration: Optional[float] = None  # Duration in seconds

class EventCoverageMetrics(BaseModel):
    """Detailed metrics for event coverage analysis"""
    total_events: int = Field(..., description="Total number of events analyzed")
    covered_events: int = Field(..., description="Number of events covered by the rule")
    coverage_score: float = Field(..., description="Event coverage score (percentage)")
    total_holiday_events: int = Field(..., description="Total number of holiday events")
    covered_holiday_events: int = Field(..., description="Number of holiday events covered")
    holiday_coverage_score: float = Field(..., description="Holiday coverage score (percentage)")
    events: List[EventDetail] = Field(default_factory=list, description="Details about each event")

class AlertMetrics(BaseModel):
    """Detailed metrics for alert analysis"""
    total_alerts: int = Field(..., description="Total number of alerts generated")
    total_resources: int = Field(..., description="Total number of unique resources")
    open_alerts: int = Field(..., description="Number of alerts still open")
    open_alert_score: float = Field(..., description="Open alert score (percentage)")
    alert_duration_score: float = Field(..., description="Alert duration score (percentage)")
    simulation_times: int = Field(..., description="Number of simulation times used")
    alerts: List[AlertDetail] = Field(default_factory=list, description="Details about each alert")

class ReliabilityMetrics(BaseModel):
    """Complete reliability metrics including all component scores"""
    rule_id: str = Field(..., description="ID of the analyzed rule")
    event_coverage: EventCoverageMetrics = Field(..., description="Event coverage metrics")
    alert_metrics: AlertMetrics = Field(..., description="Alert metrics")
    final_score: float = Field(..., description="Final weighted reliability score")
    execution_time: float = Field(..., description="Total execution time in seconds")
