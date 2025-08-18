import time
from datnguyen.rule_auditor.scoring.event_coverage import event_coverage
from datnguyen.rule_auditor.scoring.simulate_rule import test_rule
from datnguyen.rule_auditor.scoring.models import ReliabilityMetrics
from monitoring_platform.sdk.logger import get_logger

logger = get_logger(__name__)


def scoring_rule(rule, events, start_date, end_date) -> ReliabilityMetrics:
    """Calculate reliability score for a rule using pre-collected events.

    Args:
        rule: The rule object to score
        events: Pre-collected events list
        start_date: Start date for analysis (timezone-aware)
        end_date: End date for analysis (timezone-aware)

    Returns:
        ReliabilityMetrics: Complete reliability metrics including all component scores
    """
    start_time = time.time()
    logger.info(f"Starting reliability scoring for rule {rule.id} with pre-collected events")

    # Calculate event coverage scores using provided events
    logger.info("Calculating event coverage scores with pre-collected events...")
    event_coverage_metrics = event_coverage(rule, events, start_date, end_date)
    logger.user(
        f"Event coverage scores - Event: {event_coverage_metrics.coverage_score:.2f}, Holiday: {event_coverage_metrics.holiday_coverage_score:.2f}")

    # Calculate alert scores using provided events
    logger.info("Calculating alert scores with pre-collected events...")
    alert_metrics = test_rule(rule, events, start_date, end_date)
    logger.user(
        f"Alert scores - Duration: {alert_metrics.alert_duration_score:.2f}, Open: {alert_metrics.open_alert_score:.2f}")

    # Calculate final weighted score
    final_score = (
                          alert_metrics.alert_duration_score * 0.75 +
                          alert_metrics.open_alert_score * 0.75 +
                          event_coverage_metrics.coverage_score * 1 +
                          event_coverage_metrics.holiday_coverage_score * 0.5
                  ) / 3

    metrics = ReliabilityMetrics(
        rule_id=str(rule.id),
        event_coverage=event_coverage_metrics,
        alert_metrics=alert_metrics,
        final_score=final_score,
        execution_time=time.time() - start_time
    )

    logger.user(f"Final score for rule {rule.id} with pre-collected events: {metrics.final_score:.2f}")
    return metrics
