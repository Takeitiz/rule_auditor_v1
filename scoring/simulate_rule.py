from curses.ascii import TAB
import datetime
import traceback
import os

from datnguyen.rule_auditor.scoring.models import AlertDetail, AlertMetrics
from datnguyen.rule_auditor.scoring.utils import enable_profiling, generate_important_simulation_times, \
    patch_inmem_config, patch_rule
from monitoring_platform.sdk.dependency_injection.injector import inject_dependencies
from monitoring_platform.sdk.dependency_injection.time_traveler import TimeTraveler
from monitoring_platform.sdk.exceptions import RuleDeferredException
from monitoring_platform.sdk.externals.client.alert_manager_client import AlertManagerQueryBuilder
from monitoring_platform.sdk.factory.const import CUSTOM_MONITOR_RULE, OPEN_GRAPH_JOB_MONITOR_RULE, \
    TABLE_SERVICE_MONITOR_RULE
from monitoring_platform.sdk.factory.monitor_factory import MonitoringFactory
from monitoring_platform.sdk.logger import get_logger
from monitoring_platform.sdk.metric_collector.custom_file_monitor_metric_collector import \
    SimulateFileMonitorMetricCollector
from monitoring_platform.sdk.monitor import Monitor

logger = get_logger(__name__)

SDK_INMEM_CONFIG = "/home/datnguyen/git/pipeline-operations/python/datnguyen/rule_auditor/inmem_config.yaml"

def simulate(simulated_date, rule, monitor, alert_manager_client):
    try:
        alerts = monitor.execute()
        if not alerts:
            return None

        # Set create_time for all alerts
        for alert in alerts:
            alert.create_time = simulated_date
            alert.environment = "Development"
            alert.event = "simulated"

        alert_manager_client.create_alert(alerts)
        return alerts
    except RuleDeferredException as e:
        return None
    except Exception as e:
        logger.error(f"Exception {rule.id}")
        traceback.print_exc()
        return None

def get_event_client(container, rule_type):
    if rule_type == CUSTOM_MONITOR_RULE:
        return container.get("file_event_client")
    if rule_type == TABLE_SERVICE_MONITOR_RULE:
        return container.get("table_service_event_client")
    if rule_type == OPEN_GRAPH_JOB_MONITOR_RULE:
        return container.get("open_graph_job_client")
    raise Exception("Unsupported rule type")


@enable_profiling("test_rule")
def test_rule(rule, events, start_date, end_date, step=1800) -> AlertMetrics:
    """Run test simulation for a rule.

    Args:
        rule: The rule to test
        start_date: Start date for simulation
        end_date: End date for simulation
        step: Time step for simulation in seconds

    Returns:
        AlertMetrics: Metrics about the alerts generated during simulation
    """
    logger.user(f"Starting test_rule for rule {rule.id}")
    # logger.info("Collecting events...")
    # events = get_all_events(rule.id, start_date, end_date)

    # Use the rule object as passed in, don't fetch it again
    rule = patch_rule(rule)

    # Get the appropriate metric collector for the rule type
    metric_collector_cls = MonitoringFactory.get_metric_collector_type(rule.type)
    if rule.type == CUSTOM_MONITOR_RULE:
        metric_collector_cls = SimulateFileMonitorMetricCollector

    metric_collector = metric_collector_cls(rule)
    monitor = Monitor(rule=rule)
    monitor.metric_collector = metric_collector
    alerts = []

    if not events:
        return AlertMetrics(
            total_alerts=0,
            total_resources=0,
            open_alerts=0,
            open_alert_score=0,
            alert_duration_score=0,
            simulation_times=0,
            alerts=[]
        )
    # Get start date considering all event types
    start_date = min(event.timestamp for event in events).replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info(f"Generating simulation times {start_date} {end_date}...")
    important_times = generate_important_simulation_times(rule, start_date, end_date, events, step)
    logger.user(f"Generated {len(important_times)} simulation times")

    for r in rule.constraints:
        if r["constraint_params"] == "file_max_age_constraint":
            r["constraint_params"]["max_age"] = r["constraint_params"]["max_age"] + 900

    with inject_dependencies(SDK_INMEM_CONFIG) as container:
        # Set events for all repository types
        event_client = get_event_client(container, rule.type)
        event_client.repository.set_events(events)
        am_client = container.get("alert_manager_client")
        logger.info("Starting rule simulation...")

        # Override important_times for debugging
        # important_times = [datetime.datetime(2025, 4, 18, 23, 30, 0, 0, tzinfo=ZoneInfo("GMT"))]
        for i, sim_time in enumerate(important_times):
            # sim_time = sim_time.astimezone(ZoneInfo("GMT"))
            # print("SIM_TIME", sim_time)
            if i % 200 == 0:
                logger.user(f"Processing simulation time {i}/{len(important_times)}")
            with TimeTraveler(sim_time):
                if hasattr(rule, 'translated_pattern'):
                    rule.translated_pattern = None
                if hasattr(rule, 'translated_partition'):
                    rule.translated_partition = None
                rule.rule_start_timestamp = None

                alert = simulate(sim_time, rule, monitor, am_client)
                if alert:
                    alerts.extend(alert)

        open_alert_score = calculate_open_alert_score(am_client)
        alert_duration_score, alert_details, open_alerts = calculate_alert_duration(am_client)
        # print(len(alert_details))
        # for a in alert_details:
        #     print(a)
        metrics = AlertMetrics(
            total_alerts=len(set(a.resource for a in alerts)),
            total_resources=len(set(a.resource for a in alerts)),
            open_alerts=open_alerts,
            open_alert_score=open_alert_score,
            alert_duration_score=alert_duration_score,
            simulation_times=len(important_times),
            alerts=alert_details
        )

    return metrics


def calculate_open_alert_score(am_client):
    """Calculate open alert score based on current alert states.

    Args:
        am_client: Alert manager client instance

    Returns:
        float: Open alert score (0-100)
    """
    # Get all alerts from the repository
    all_alerts = list(am_client.get_all(AlertManagerQueryBuilder()))
    if not all_alerts:
        return 100

    # Count resources with current non-ok severity
    resources_with_alerts = {}  # resource -> latest severity
    for alert in all_alerts:
        if not alert.history:
            continue
        # Get latest severity for this resource
        latest_entry = max(alert.history, key=lambda x: x['update_time'])
        resources_with_alerts[alert.resource] = latest_entry['severity']

    total_resources = len(resources_with_alerts)
    if total_resources == 0:
        return 100

    # Count resources that are currently in non-ok state
    non_ok_resources = sum(1 for severity in resources_with_alerts.values() if severity != 'ok')

    # Calculate score
    open_alert_score = ((total_resources - non_ok_resources) / total_resources) * 100

    logger.debug(
        f'OpenAlert Score {format(open_alert_score, ".2f")} with total_resources {total_resources}, non_ok_resources {non_ok_resources}')
    return open_alert_score


def calculate_alert_duration(am_client):
    """Calculate alert duration score and collect alert details based on alert history.

    Args:
        am_client: Alert manager client instance

    Returns:
        tuple: (
            float: Alert duration score (0-100),
            list: List of AlertDetail objects,
            int: Number of open alerts
        )
    """
    all_alerts = list(am_client.get_all(AlertManagerQueryBuilder()))
    if not all_alerts:
        return 100, [], 0

    alert_durations = []
    alert_details = []
    open_alerts = 0
    resources = set()

    for alert in all_alerts:
        if not alert.history:
            continue

        history = sorted(alert.history, key=lambda x: x['update_time'])

        current_start = None
        current_severity = None
        for entry in history:
            if current_start is None and entry['severity'] != 'ok':
                current_start = entry['update_time']
                current_severity = entry['severity']
            elif current_start is not None and entry['severity'] == 'ok':
                duration = (entry['update_time'] - current_start).total_seconds()
                alert_durations.append((alert.resource, duration))
                alert_details.append(AlertDetail(
                    resource=alert.resource,
                    severity=current_severity,
                    open_time=current_start,
                    close_time=entry['update_time'],
                    duration=duration
                ))
                current_start = None
            elif current_start is None and entry["severity"] == "ok":
                if alert.resource not in resources:
                    alert_details.append(AlertDetail(
                        resource=alert.resource,
                        severity=entry["severity"],
                        open_time=entry['update_time'],
                        close_time=entry['update_time'],
                        duration=0
                    ))

            resources.add(alert.resource)

        # Check if alert is still open (has non-ok severity at the end)
        if current_start is not None and history:
            latest_entry = history[-1]
            if latest_entry['severity'] != 'ok':
                duration = (latest_entry['update_time'] - current_start).total_seconds()
                alert_durations.append((alert.resource, duration))
                alert_details.append(AlertDetail(
                    resource=alert.resource,
                    severity=current_severity,
                    open_time=current_start,
                    close_time=None,
                    duration=duration
                ))
                open_alerts += 1

    if not alert_durations:
        return 100, alert_details, 0

    # Calculate average duration per resource
    resource_durations = {}
    for resource, duration in alert_durations:
        if resource not in resource_durations:
            resource_durations[resource] = []
        resource_durations[resource].append(duration)

    average_durations = {r: sum(d) / len(d) for r, d in resource_durations.items()}
    if len(average_durations) / len(resources) > 0.1:
        for resource in average_durations:
            if average_durations[resource] < 7200:
                average_durations[resource] = 0
        duration_score = (sum(1 for d in average_durations.values() if d == 0) / len(average_durations)) * 100
    else:
        duration_score = 100

    logger.debug(
        f'Alert duration Score {format(duration_score, ".2f")}, with total_unique_resource {len(resources)}, len_alert_duration {len(average_durations)}')

    return duration_score, sorted(alert_details, key=lambda x: x.open_time), open_alerts
