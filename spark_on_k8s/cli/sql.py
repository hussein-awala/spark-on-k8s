from __future__ import annotations

from pathlib import Path

import click

from spark_on_k8s.cli.automation import execute_submission, execution, output_option, timeout_option
from spark_on_k8s.cli.options import (
    app_name_option,
    docker_image_option,
    driver_cpu_option,
    driver_memory_option,
    driver_memory_overhead_option,
    executor_cpu_option,
    executor_initial_instances_option,
    executor_memory_option,
    executor_memory_overhead_option,
    image_pull_policy_option,
    logs_option,
    namespace_option,
    service_account_option,
    spark_conf_option,
    wait_option,
)
from spark_on_k8s.cli.receipts import receipt_option


@click.group(name="sql", help="Submit SQL files or inline statements using a PySpark image.")
def sql_cli():
    pass


@sql_cli.command(
    params=[
        docker_image_option,
        namespace_option,
        service_account_option,
        app_name_option,
        spark_conf_option,
        wait_option,
        logs_option,
        image_pull_policy_option,
        driver_cpu_option,
        driver_memory_option,
        driver_memory_overhead_option,
        executor_cpu_option,
        executor_memory_option,
        executor_memory_overhead_option,
        executor_initial_instances_option,
    ],
    help="Submit exactly one SQL file or inline SQL string. Statements execute in order.",
)
@output_option
@timeout_option
@receipt_option
@click.option("--file", "sql_file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--sql", type=str, help="Inline SQL; quote this value to protect it from shell expansion.")
def submit(
    sql: str | None, sql_file: Path | None, output: str, timeout: float | None, receipt_file, **options
):
    from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
    from spark_on_k8s.sql import prepare_sql_options

    if (sql is None) == (sql_file is None):
        raise click.UsageError("Provide exactly one of --sql or --file")
    logs, wait = options.pop("logs"), options.pop("wait")
    waiter = "log" if logs else "wait" if wait else "no_wait"
    options["app_name"] = options.pop("name")
    for role in ("driver", "executor"):
        options[f"{role}_resources"] = PodResources(
            cpu=options.pop(f"{role}_cpu"),
            memory=options.pop(f"{role}_memory"),
            memory_overhead=options.pop(f"{role}_memory_overhead"),
        )
    options["executor_instances"] = ExecutorInstances(initial=options.pop("executor_initial_instances"))
    try:
        prepared = prepare_sql_options(sql=sql, sql_file=sql_file, app_waiter=waiter, **options)
    except (ImportError, OSError, ValueError, TypeError):
        # Preserve the same single-document runtime error format as other submissions.
        with execution("submit", options["namespace"], output) as result:
            result.update(run=None, status=None)
            raise
    else:
        execute_submission(SparkOnK8S(), prepared, output=output, timeout=timeout, receipt_file=receipt_file)
