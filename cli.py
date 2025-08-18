import click
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import traceback
import logging
import multiprocessing
import concurrent.futures
import time as time_module
import traceback
import functools
from tqdm import tqdm
from monitoring_platform.sdk.dependency_injection.injector import auto_inject
from monitoring_platform.sdk.externals.client.monitoring_server_client import (
    MonitoringServerClient,
    MonitoringServerQueryBuilder
)
from monitoring_platform.sdk.logger import get_logger
from datnguyen.rule_auditor.storage.base import StorageManager, StorageKey
from datnguyen.rule_auditor.storage.file_storage import FileStorageBackend
from datnguyen.rule_auditor.workflow import RuleComponentFactory
from datnguyen.rule_auditor.config import (
    DEFAULT_CONFIG,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_ANALYSIS_FILENAME,
    DEFAULT_MAX_WORKERS
)

logger = get_logger(__name__)

def get_workflow(rule, tz):
    """Get appropriate workflow for rule type"""
    return RuleComponentFactory.create_workflow(rule, tz)

def split_rule_ids(ctx, param, value):
    """Split comma-separated rule IDs into a list."""
    if value:
        return value.split(',')
    return []

def setup_debug_logging(debug: bool) -> None:
    """Configure debug logging if requested."""
    if debug:
        # Set root logger and all loggers to DEBUG level
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        # Force immediate output
        logging.getLogger().handlers[0].flush()

def fetch_rules(
    rule_id: Optional[str],
    rule_ids: List[str],
    monitoring_server_client: MonitoringServerClient
) -> List:
    """Fetch rules based on provided criteria."""
    if rule_id:
        return [monitoring_server_client.get_rule(id=rule_id)]
    elif rule_ids:
        return [monitoring_server_client.get_rule(id=rid) for rid in rule_ids]
    else:
        qb = (MonitoringServerQueryBuilder()
            .filter_status(True)
            .filter_custom_file_rule()
            .limit(5000)
            .raw(False)
        )
        rules = list(monitoring_server_client.get_all(qb))
        click.echo(f"Found {len(rules)} active rules")
        return rules


def store_analysis_results(
        storage: StorageManager,
        rule,
        analysis_results: dict,
        start_date: datetime,
        end_date: datetime
) -> None:
    """Store analysis results (statistics, suggestions, and scoring) to storage."""
    # Store statistics
    if "statistics" in analysis_results:
        logger.info("Storing statistics...")
        try:
            storage.backend.store(
                StorageKey(
                    rule_id=rule.id,
                    data_type='statistics',
                    start_date=start_date,
                    end_date=end_date
                ),
                analysis_results["statistics"]
            )
            logger.info("Statistics stored successfully")
        except Exception as e:
            logger.error(f"Error storing statistics: {str(e)}")

    # Store suggestions
    if "suggestions" in analysis_results:
        logger.info("Storing suggestions...")
        try:
            storage.backend.store(
                StorageKey(
                    rule_id=rule.id,
                    data_type='suggestions',
                    start_date=start_date,
                    end_date=end_date
                ),
                analysis_results["suggestions"]
            )
            logger.info("Suggestions stored successfully")
        except Exception as e:
            logger.error(f"Error storing suggestions: {str(e)}")

    # Store reliability score before applying suggestions
    if "original_score" in analysis_results:
        logger.info("Storing reliability score (before)...")
        try:
            storage.backend.store(
                StorageKey(
                    rule_id=rule.id,
                    data_type='reliability_metric_before',
                    start_date=start_date,
                    end_date=end_date
                ),
                analysis_results["original_score"]
            )
            logger.info("Reliability score (before) stored successfully")
        except Exception as e:
            logger.error(f"Error storing reliability score (before): {str(e)}")

    # Store reliability score after applying suggestions
    if "suggested_score" in analysis_results:
        logger.info("Storing reliability score (after)...")
        try:
            storage.backend.store(
                StorageKey(
                    rule_id=rule.id,
                    data_type='reliability_metric_after',
                    start_date=start_date,
                    end_date=end_date
                ),
                analysis_results["suggested_score"]
            )
            logger.info("Reliability score (after) stored successfully")
        except Exception as e:
            logger.error(f"Error storing reliability score (after): {str(e)}")


def _analysis_worker(
        rule_id: str,
        start_date: datetime,
        end_date: datetime,
        output_dir: str,
        attribute: Optional[str],
        step: str,
        tz: Optional[str]
):
    """
    Worker function to process a single rule.
    Initializes its own storage to be process-safe.
    """
    try:
        start_time = time_module.time()

        # Each worker creates its own StorageManager to avoid race conditions
        storage = StorageManager(FileStorageBackend(output_dir))

        rule = get_rule(rule_id)
        workflow = get_workflow(rule, tz)

        analysis_results = workflow.analyze_rule(
            rule, tz, start_date, end_date, step, attribute
        )

        store_analysis_results(storage, rule, analysis_results, start_date, end_date)

        execution_time = time_module.time() - start_time

        # For parallel mode, we return scores for the summary file.
        # For single mode, this return value is ignored.
        original_score = analysis_results.get('original_score')
        suggested_score = analysis_results.get('suggested_score')

        return (
            rule_id,
            original_score.final_score if original_score else "N/A",
            suggested_score.final_score if suggested_score else "N/A",
            execution_time
        )
    except Exception as e:
        logger.error(f"Error processing rule {rule_id}: {str(e)}\n{traceback.format_exc()}")
        return (rule_id, "ERROR", "ERROR", 0.0)

def writer_process(results_queue, output_file):
    """Write results to file from queue."""
    with open(output_file, 'a') as f:
        while True:
            result = results_queue.get()
            if result == "STOP":
                break
            rule_id, original_score, suggested_score, execution_time = result
            f.write(f"{rule_id}|{original_score}|{suggested_score}|{execution_time:.2f}\n")
            f.flush()


def run_parallel_analysis(
        rules: List,
        start_date: datetime,
        end_date: datetime,
        output_dir: str,
        output_file: str,
        attribute: Optional[str],
):
    """Run analysis in parallel using multiple processes."""
    click.echo("Parallel analysis started")

    # --- Caching Implementation ---
    processed_rule_ids = set()
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            for line in f:
                try:
                    # Strip whitespace and ensure string type
                    rule_id = line.split('|')[0].strip()
                    if rule_id:
                        processed_rule_ids.add(str(rule_id))
                except IndexError:
                    # Ignore malformed lines
                    continue

    if processed_rule_ids:
        click.echo(f"Found {len(processed_rule_ids)} rules in the cache file.")

        # --- Debugging ---
        click.echo("--- Debugging Cache ---")
        click.echo(f"Sample cached IDs: {list(processed_rule_ids)[:5]}")
        click.echo(f"Sample fetched rule IDs: {[str(rule.id) for rule in rules[:5]]}")
        click.echo("----------------------")
        # --- End Debugging ---

        # Ensure all rule IDs from the server are strings for comparison
        all_rule_ids = {str(rule.id) for rule in rules}

        rules_to_process_ids = all_rule_ids - processed_rule_ids
        rules_to_process = [rule for rule in rules if str(rule.id) in rules_to_process_ids]

        click.echo(f"Skipping {len(rules) - len(rules_to_process)} cached rules.")
    else:
        rules_to_process = rules
    # --- End Caching Implementation ---

    results_queue = multiprocessing.Queue()
    writer = multiprocessing.Process(target=writer_process, args=(results_queue, output_file))
    writer.start()

    # In parallel mode, we intentionally run the full 'scorev2' workflow
    step_for_parallel = 'scorev2'
    tz_for_parallel = None
    click.echo(f"Parallel mode will run with step='{step_for_parallel}' and tz='{tz_for_parallel}' for all rules.")

    with concurrent.futures.ProcessPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                _analysis_worker,
                rule.id,
                start_date,
                end_date,
                output_dir,
                attribute,
                step_for_parallel,
                tz_for_parallel
            ) for rule in rules_to_process
        ]

        if not futures:
            click.echo("No new rules to process. Exiting.")
            results_queue.put("STOP")
            writer.join()
            return

        with tqdm(total=len(futures), desc='Analyzing rules') as progress:
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    results_queue.put(result)
                progress.update(1)

    results_queue.put("STOP")
    writer.join()

# @functools.lru_cache(maxsize=1000)
@auto_inject(DEFAULT_CONFIG)
def get_rule(rule_id, monitoring_server_client: MonitoringServerClient, is_raw=False, **kwargs):
    return monitoring_server_client.get_rule(id=rule_id, is_raw=is_raw)

@click.group()
def cli():
    """Rule Audit CLI"""
    pass

@cli.command()
@click.option(
    '--parallel',
    is_flag=True,
    default=False,
    help='Run analysis in parallel using multiple processes'
)
@click.option(
    '--output-file',
    type=click.Path(),
    default=DEFAULT_ANALYSIS_FILENAME,
    help='Output file for analysis results'
)
@click.option(
    '--rule-id',
    help='Single Rule ID to analyze'
)
@click.option(
    '--rule-ids',
    default='',
    callback=split_rule_ids,
    help='Comma-separated list of Rule IDs to analyze'
)
@click.option(
    '--start-date',
    type=click.DateTime(formats=["%Y%m%d"]),
    default=lambda: (datetime.now() - timedelta(days=300)).strftime("%Y%m%d"),
    help='Start date (YYYYMMDD)'
)
@click.option(
    '--end-date',
    type=click.DateTime(formats=["%Y%m%d"]),
    default=lambda: datetime.now().strftime("%Y%m%d"),
    help='End date (YYYYMMDD)'
)
@click.option(
    '--output-dir',
    type=click.Path(),
    default=str(DEFAULT_OUTPUT_DIR),
    help='Output directory for results'
)
@click.option(
    '--step',
    type=click.Choice(['collector', 'scorev1', 'builder', 'statistic', 'suggestion', 'scorev2']),
    default='suggestion',
    help='Step to run'
)
@click.option(
    '--attribute',
    type=click.Choice([
        # File monitoring attributes
        'timezone', 'check_windows', 'file_size', 'file_count', 'file_age', 'file_ownership',
        # Table service attributes (TODO: Add actual attributes)
        # 'query_timeout', 'batch_size', 'retry_count',
        # OG job attributes (TODO: Add actual attributes)
        # 'job_timeout', 'max_retries', 'concurrency'
    ]),
    help='Specific attribute to analyze'
)
@click.option(
    '--tz',
    default=None,
    help='Timezone'
)
@click.option(
    '--debug',
    is_flag=True,
    default=False,
    help='Enable debug logging'
)
@auto_inject(DEFAULT_CONFIG)
def analyze(
        rule_id: Optional[str],
        rule_ids: List[str],
        start_date: datetime,
        end_date: datetime,
        output_dir: str,
        monitoring_server_client: MonitoringServerClient,
        step: str,
        attribute: Optional[str],
        tz: Optional[str],
        debug: bool,
        parallel: bool = False,
        output_file: str = DEFAULT_ANALYSIS_FILENAME
):
    """Analyze rules and generate suggestions"""
    # Setup logging
    setup_debug_logging(debug)

    # Display startup information
    click.echo("Starting rule analysis...")
    click.echo(f"Environment setup: CORTEX_PROJECT={os.getenv('CORTEX_PROJECT', 'Not set')}")

    try:
        # Fetch rules to analyze
        rules = fetch_rules(rule_id, rule_ids, monitoring_server_client)

        # Choose execution mode
        if parallel:
            run_parallel_analysis(
                rules,
                start_date,
                end_date,
                output_dir,
                output_file,
                attribute,
            )
            click.echo(f"\nParallel analysis complete. Summary saved to: {output_file}")
            click.echo(f"Detailed results saved in: {output_dir}")
        else:
            # Single-threaded mode
            click.echo("Running analysis in single-threaded mode...")
            with click.progressbar(rules, label='Analyzing rules') as rules_bar:
                for rule in rules_bar:
                    _analysis_worker(
                        rule.id,
                        start_date,
                        end_date,
                        output_dir,
                        attribute,
                        step,
                        tz
                    )
            click.echo(f"\nAnalysis complete. Detailed results saved in: {output_dir}")

    except Exception as e:
        traceback.print_exc()
        click.echo(f"Error during analysis: {str(e)}", err=True)
        raise click.Abort()

if __name__ == '__main__':
    cli()
