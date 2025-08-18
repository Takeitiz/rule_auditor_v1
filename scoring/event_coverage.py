from pytz import timezone
import logging
from datetime import datetime, timedelta
from datnguyen.rule_auditor.scoring.models import EventCoverageMetrics, EventDetail
from datnguyen.rule_auditor.scoring.utils import enable_profiling, patch_rule
from monitoring_platform.sdk.dependency_injection.injector import inject_dependencies
from monitoring_platform.sdk.dependency_injection.time_traveler import TimeTraveler
from monitoring_platform.sdk.exceptions import RuleDeferredException
from monitoring_platform.sdk.factory.monitor_factory import MonitoringFactory
from monitoring_platform.sdk.logger import get_logger
from monitoring_platform.sdk.utils.datetime_helper import load_holidays_file

logger = logging.getLogger(__file__)


@enable_profiling("event_coverage")
def event_coverage(rule, events, start_date, end_date, **kwargs) -> EventCoverageMetrics:
    """Calculate event coverage scores for a rule.

    Args:
        rule: The rule object to analyze
        start_date: Start date for analysis (timezone-aware)
        end_date: End date for analysis (timezone-aware)

    Returns:
        EventCoverageMetrics: Detailed metrics about event coverage
    """
    logger.info(f"Starting event coverage analysis for rule {rule.id}")
    holidays = load_holidays_file()

    # Patch rule for analysis
    rule = patch_rule(rule)
    total = 0
    covered = 0
    num_holidays_inrange = set()
    num_event_in_holiday = 0
    num_event_in_holiday_covered = 0

    # logger.info("Collecting events for coverage analysis...")
    # events = get_all_events(rule.id, start_date, end_date)
    logger.info(f"Found {len(events)} events to analyze")

    rule_preprocessor = MonitoringFactory.get_preprocessor(rule.type)()
    rule_tz = timezone(rule.timezone.value.name)
    logger.info("Analyzing event coverage...")

    event_details = []
    with inject_dependencies() as container:
        for _, event in enumerate(events):
            total += 1
            event_target = getattr(event, 'file_name', None) or getattr(event, 'tableName', None) or str(
                getattr(event, 'db_keys', None))
            date_from_mtime = datetime.fromtimestamp(event.timestamp.timestamp(), tz=timezone("GMT"))
            date_from_mtime_tzoned = date_from_mtime.astimezone(rule_tz)
            with TimeTraveler(date_from_mtime):
                try:
                    # Only set translated_pattern for FileEvent rules
                    if hasattr(rule, 'translated_pattern'):
                        rule.translated_pattern = None
                    if hasattr(rule, 'translated_partition'):
                        rule.translated_partition = None
                    rule_preprocessor.preprocess(rule)
                    is_holiday = bool(
                        rule.country and holidays.get(rule.country_code).get(date_from_mtime_tzoned.strftime("%Y%m%d")))
                    if is_holiday:
                        num_holidays_inrange.add(date_from_mtime.strftime("%Y%m%d"))
                        num_event_in_holiday_covered += 1
                    covered += 1
                except RuleDeferredException as e:
                    date_from_mtime_tzoned = date_from_mtime.astimezone(rule_tz)
                    start_time_dt = datetime.combine(date_from_mtime_tzoned.date(), datetime.min.time()) + timedelta(
                        seconds=rule.start_time)
                    end_time_dt = datetime.combine(date_from_mtime_tzoned.date(), datetime.min.time()) + timedelta(
                        seconds=rule.end_time)
                    start_time_dt = rule_tz.localize(start_time_dt)
                    end_time_dt = rule_tz.localize(end_time_dt)
                    if "fall within TimeWindow" in str(e):
                        if (date_from_mtime_tzoned + timedelta(hours=2) >= start_time_dt) or (
                                date_from_mtime_tzoned + timedelta(hours=2) >= end_time_dt):
                            covered += 1
                            event_details.append(EventDetail(
                                file_name=event_target,
                                timestamp=date_from_mtime_tzoned,
                                is_covered=True,
                                is_holiday=False,
                                reason=f'covered within 2 hours of start/end time {date_from_mtime_tzoned} {start_time_dt} {end_time_dt}'
                            ))
                        else:
                            event_details.append(EventDetail(
                                file_name=event_target,
                                timestamp=date_from_mtime_tzoned,
                                is_covered=False,
                                is_holiday=False,
                                reason=str(e)
                            ))
                    elif "holiday" in str(e):
                        num_event_in_holiday += 1
                        event_details.append(EventDetail(
                            file_name=event_target,
                            timestamp=date_from_mtime_tzoned,
                            is_covered=False,
                            is_holiday=True,
                            reason=str(e)
                        ))
                    elif "fall within WeekdayWindow" in str(e):
                        event_details.append(EventDetail(
                            file_name=event_target,
                            timestamp=date_from_mtime_tzoned,
                            is_covered=False,
                            is_holiday=False,
                            reason=f'{date_from_mtime_tzoned.strftime("%A")} --- {str(e)}'
                        ))
                    else:
                        event_details.append(EventDetail(
                            file_name=event_target,
                            timestamp=date_from_mtime_tzoned,
                            is_covered=False,
                            is_holiday=False,
                            reason=str(e)
                        ))
                    continue

    if num_event_in_holiday == 0:
        num_event_in_holiday = 1
        num_event_in_holiday_covered = 1

    if total == 0:
        event_covered_score = 0
        holiday_covered_score = 0
    else:
        event_covered_score = covered / total * 100
        holiday_covered_score = num_event_in_holiday_covered / num_event_in_holiday * 100

    metrics = EventCoverageMetrics(
        total_events=total,
        covered_events=covered,
        coverage_score=event_covered_score,
        total_holiday_events=num_event_in_holiday,
        covered_holiday_events=num_event_in_holiday_covered,
        holiday_coverage_score=holiday_covered_score,
        events=sorted(event_details, key=lambda x: x.timestamp)
    )

    logger.info(f"Event coverage analysis complete:")
    logger.info(f"- Event coverage: {metrics.coverage_score:.2f}% ({metrics.covered_events}/{metrics.total_events} events covered)")
    logger.info(f"- Holiday coverage: {metrics.holiday_coverage_score:.2f}% ({metrics.covered_holiday_events}/{metrics.total_holiday_events} holiday events covered)")

    return metrics
