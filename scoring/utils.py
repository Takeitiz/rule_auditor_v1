from datetime import datetime, timedelta
import functools
from zoneinfo import ZoneInfo
from pytz import timezone
import cProfile
import pstats
# from datnguyen.rule_auditor.workflow import RuleComponentFactory
from monitoring_platform.sdk.dependency_injection.config.alert_manager_client_config import \
    AlertManagerInMemoryRepositoryConfig
from monitoring_platform.sdk.dependency_injection.config.file_event_client_config import FileEventInMemRepositoryConfig
from monitoring_platform.sdk.dependency_injection.config.table_service_event_client_config import \
    TableServiceEventInMemRepositoryConfig
from monitoring_platform.sdk.dependency_injection.config.open_graph_job_client_config import \
    OpenGraphJobInMemRepositoryConfig
from monitoring_platform.sdk.dependency_injection.config.sdk_config import SDKConfig
from monitoring_platform.sdk.dependency_injection.injector import auto_inject
from monitoring_platform.sdk.externals.client.monitoring_server_client import MonitoringServerClient
from monitoring_platform.sdk.logger import get_logger

logger = get_logger(__name__)


def enable_profiling(func_name: str):
    """Decorator to enable/disable profiling of a function.

    Args:
        func_name: Name of the function being profiled, used in output file name
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get profiling settings from global state
            enabled = getattr(wrapper, 'enabled', False)
            output_base = getattr(wrapper, 'output_base', None)

            if not enabled:
                return func(*args, **kwargs)

            profiler = cProfile.Profile()
            try:
                profiler.enable()
                result = func(*args, **kwargs)
                profiler.disable()

                stats = pstats.Stats(profiler)
                stats.sort_stats('cumulative')

                if output_base:
                    # Generate unique output path for this function
                    output_path = f"{output_base}_{func_name}.prof"
                    stats.dump_stats(output_path)
                    logger.info(f"Profile data for {func_name} written to {output_path}")
                else:
                    logger.info(f"Profile data for {func_name}:")
                    stats.print_stats(30)

                return result
            finally:
                profiler.disable()

        return wrapper

    return decorator


def generate_important_simulation_times(rule, start_date, end_date, events, step=1800):
    """
    Generate important times to simulate a monitoring rule based on rule window configurations.

    Args:
        rule: The monitoring rule to simulate
        start_date: The start date of the simulation period
        end_date: The end date of the simulation period
        events: List of file events
        step: Default step size in seconds for filling gaps

    Returns:
        list: Sorted list of datetime objects representing important times to simulate
    """
    simulation_days = (end_date - start_date).days
    events_per_day = len(events) / simulation_days if simulation_days > 0 else len(events)
    # print("Event Perday", events_per_day)
    # If more than 30 events per day on average, switch to step-based simulation
    if events_per_day > 30:
        important_times = []
        current = start_date
        while current <= end_date:
            important_times.append(current)
            current += timedelta(seconds=step)
        return important_times

    # Extract timezone from rule
    rule_tz = timezone(rule.timezone.value.name) if hasattr(rule, 'timezone') and hasattr(rule.timezone,
                                                                                          'value') else timezone('UTC')

    # Start with empty set to avoid duplicates
    important_times = set()

    # Convert start_date and end_date to rule's timezone
    start_date_tz = start_date.astimezone(rule_tz)
    end_date_tz = end_date.astimezone(rule_tz)

    # Get window configurations
    window_includes = getattr(rule, 'window_include', []) or []
    window_excludes = getattr(rule, 'window_exclude', []) or []

    # Process WeekdayWindow configurations
    weekday_windows = [w for w in window_includes + window_excludes if
                       hasattr(w, 'type') and w.type == "weekday_window"]
    weekday_sets = []
    for window in weekday_windows:
        weekday_sets.append(set(window.weekdays))

    # Process CheckDateTimeWindow configurations
    datetime_windows = [w for w in window_includes + window_excludes if hasattr(w, 'type') and w.type == "check_datetime_window"]

    # Add times for each day in the range
    current_date = start_date_tz.replace(hour=0, minute=0, second=0, microsecond=0)
    while current_date <= end_date_tz:
        weekday = str((current_date.weekday() + 1) % 7)
        is_weekday_match = False
        for weekday_set in weekday_sets:
            if weekday in weekday_set:
                is_weekday_match = True
                break

        # If timezone is defined and rule has start/end times, add those times
        if hasattr(rule, 'start_time') and rule.start_time is not None:
            day_start_time = datetime.combine(current_date.date(), datetime.min.time()) + timedelta(
                seconds=rule.start_time)
            day_start_time = rule_tz.localize(day_start_time).astimezone(ZoneInfo("GMT"))
            important_times.add(day_start_time)

        if hasattr(rule, 'end_time') and rule.end_time is not None:
            day_end_time = datetime.combine(current_date.date(), datetime.min.time()) + timedelta(seconds=rule.end_time)
            day_end_time = rule_tz.localize(day_end_time).astimezone(ZoneInfo("GMT"))
            important_times.add(day_end_time)

        # Process datetime windows for this day
        for dt_window in datetime_windows:
            for dt_range in dt_window.check_datetime_ranges:
                # Add start and end times if they fall within our simulation period
                if dt_range.start_datetime <= end_date_tz and dt_range.end_datetime >= start_date_tz:
                    # Localize times if needed
                    start_dt = dt_range.start_datetime.replace(
                        tzinfo=rule_tz) if dt_range.start_datetime.tzinfo is None else dt_range.start_datetime
                    end_dt = dt_range.end_datetime.replace(
                        tzinfo=rule_tz) if dt_range.end_datetime.tzinfo is None else dt_range.end_datetime
                    # Convert to GMT for consistency
                    start_dt = start_dt.astimezone(ZoneInfo("GMT"))
                    end_dt = end_dt.astimezone(ZoneInfo("GMT"))
                    important_times.add(start_dt)
                    important_times.add(end_dt)

        # Move to next day
        current_date += timedelta(days=1)

    # Add event times and surrounding times
    for event in events:
        # Only process events within our simulation period
        if start_date <= event.timestamp <= end_date:
            # print(event.db_keys.timestamp, event.db_keys.action, event.db_keys.retry, event.job_status, event.event_time, event.timestamp)
            important_times.add(event.timestamp - timedelta(minutes=5))
            important_times.add(event.timestamp + timedelta(minutes=5))

    # Convert the set to a sorted list
    important_times_list = sorted(list(important_times))
    # Filter times to be within the original range
    important_times_list = [t for t in important_times_list if start_date <= t <= end_date]

    return important_times_list


def patch_rule(rule):
    """Patch rule for analysis based on rule type"""
    # Only set use_file_event for FileEvent rules
    if hasattr(rule, 'use_file_event'):
        rule.use_file_event = 1

    # Remove DateThreshold from window_exclude for all rule types
    if hasattr(rule, 'window_exclude') and rule.window_exclude:
        for r in rule.window_exclude:
            if "DateThreshold" in r.__class__.__name__:
                rule.window_exclude.remove(r)

    return rule


# def get_all_events(rule_id, start_date, end_date):
#     """Get all events for a rule using the main collector system."""
#     rule = get_rule(rule_id)
#     components = RuleComponentFactory.RULE_TYPE_MAPPING[rule.type]
#     collector = components["collector"]
#     return collector.collect_events(rule, start_date, end_date)

@functools.lru_cache(maxsize=1000)
@auto_inject()
def get_rule(rule_id, monitoring_server_client: MonitoringServerClient, is_raw=False, **kwargs):
    return monitoring_server_client.get_rule(id=rule_id, is_raw=is_raw)


def patch_inmem_config(config_file):
    config = SDKConfig.from_yaml(config_file)

    # Directly patch all repositories to in-memory
    config.alert_manager.repo = AlertManagerInMemoryRepositoryConfig()
    config.file_event_client.repo = FileEventInMemRepositoryConfig()
    config.table_service_event_client.repo = TableServiceEventInMemRepositoryConfig()
    config.open_graph_job_client.repo = OpenGraphJobInMemRepositoryConfig()

    return config
