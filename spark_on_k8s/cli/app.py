from __future__ import annotations

from typing import Literal

import click

from spark_on_k8s.cli.automation import execute_submission, execution, output_option, timeout_option
from spark_on_k8s.cli.options import (
    app_id_option,
    app_name_option,
    app_path_option,
    class_name_option,
    docker_image_option,
    driver_annotations_option,
    driver_container_name_option,
    driver_cpu_option,
    driver_env_vars_from_secrets_option,
    driver_labels_option,
    driver_memory_option,
    driver_memory_overhead_option,
    driver_node_selector_option,
    driver_pod_template_option,
    executor_allocation_ratio_option,
    executor_annotations_option,
    executor_cpu_option,
    executor_initial_instances_option,
    executor_labels_option,
    executor_max_instances_option,
    executor_memory_option,
    executor_memory_overhead_option,
    executor_min_instances_option,
    executor_node_selector_option,
    executor_pod_template_path_option,
    executor_template_option,
    force_option,
    image_pull_policy_option,
    logs_option,
    namespace_option,
    scheduler_backlog_timeout_option,
    secret_env_var_option,
    service_account_option,
    spark_conf_option,
    sustained_scheduler_backlog_timeout_option,
    ui_reverse_proxy_option,
    wait_option,
)
from spark_on_k8s.cli.receipts import receipt_option


@click.group(name="app", help="Create and manage a single Spark applications.")
def app_cli():
    pass


class SparkAppCommand(click.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.insert(0, app_id_option)
        self.params.insert(1, namespace_option)


@app_cli.command(
    cls=SparkAppCommand,
    help="Get the status of a Spark application.",
)
@output_option
def status(app_id: str, namespace: str, output: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    with execution("status", namespace, output) as result:
        result["app_id"] = app_id
        result["status"] = None
        result["status"] = SparkAppManager().app_status(namespace=namespace, app_id=app_id)
        if output == "text":
            click.echo(result["status"].value)


@app_cli.command(cls=SparkAppCommand, help="Stream the logs of a Spark application.")
def logs(app_id: str, namespace: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    app_manager.stream_logs(namespace=namespace, app_id=app_id, should_print=True)


@app_cli.command(cls=SparkAppCommand, help="Kill a Spark application.")
def kill(app_id: str, namespace: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    app_manager.kill_app(namespace=namespace, app_id=app_id)


@app_cli.command(
    cls=SparkAppCommand,
    params=[force_option],
    help="Delete a Spark application.",
)
def delete(app_id: str, namespace: str, force: bool):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    app_manager.delete_app(namespace=namespace, app_id=app_id, force=force)


@app_cli.command(cls=SparkAppCommand, help="Wait for a Spark application to finish.")
@output_option
@timeout_option
def wait(app_id: str, namespace: str, output: str, timeout: float | None):
    from spark_on_k8s.utils.app_manager import SparkAppManager
    from spark_on_k8s.utils.run import WaitDeadline

    app_manager = SparkAppManager()
    with execution("wait", namespace, output) as result:
        deadline = WaitDeadline(timeout)
        result["app_id"] = app_id
        result["status"] = None
        app_manager.wait_for_app(
            namespace=namespace,
            app_id=app_id,
            should_print=True,
            **({"timeout": deadline.remaining()} if timeout is not None else {}),
        )
        result["status"] = app_manager.app_status(
            namespace=namespace,
            app_id=app_id,
            **({"request_timeout": deadline.remaining()} if timeout is not None else {}),
        )
        deadline.remaining()
        if output == "text":
            click.echo(f"App {app_id} has terminated with status {result['status'].value}")


@app_cli.command(
    params=[
        docker_image_option,
        app_path_option,
        namespace_option,
        service_account_option,
        app_name_option,
        spark_conf_option,
        class_name_option,
        wait_option,
        logs_option,
        image_pull_policy_option,
        ui_reverse_proxy_option,
        driver_cpu_option,
        driver_memory_option,
        driver_memory_overhead_option,
        executor_cpu_option,
        executor_memory_option,
        executor_memory_overhead_option,
        executor_min_instances_option,
        executor_max_instances_option,
        executor_initial_instances_option,
        executor_allocation_ratio_option,
        scheduler_backlog_timeout_option,
        sustained_scheduler_backlog_timeout_option,
        secret_env_var_option,
        driver_env_vars_from_secrets_option,
        driver_node_selector_option,
        executor_node_selector_option,
        driver_labels_option,
        executor_labels_option,
        driver_annotations_option,
        executor_annotations_option,
        executor_pod_template_path_option,
        executor_template_option,
        driver_pod_template_option,
        driver_container_name_option,
    ],
    help="Submit a Spark application.",
)
@output_option
@timeout_option
@receipt_option
@click.argument("app_arguments", nargs=-1, type=str)
def submit(
    image: str,
    path: str,
    namespace: str,
    service_account: str,
    name: str | None,
    spark_conf: dict[str, str],
    class_name: str | None,
    wait: bool,
    logs: bool,
    image_pull_policy: Literal["Always", "Never", "IfNotPresent"],
    ui_reverse_proxy: bool,
    app_arguments: tuple[str, ...],
    driver_cpu: int,
    driver_memory: int,
    driver_memory_overhead: int,
    executor_cpu: int,
    executor_memory: int,
    executor_memory_overhead: int,
    executor_min_instances: int,
    executor_max_instances: int,
    executor_initial_instances: int,
    executor_allocation_ratio: float,
    scheduler_backlog_timeout: str,
    sustained_scheduler_backlog_timeout: str,
    secret_env_var: dict[str, str],
    driver_env_vars_from_secrets: list[str],
    driver_node_selector: dict[str, str],
    executor_node_selector: dict[str, str],
    driver_labels: dict[str, str],
    executor_labels: dict[str, str],
    driver_annotations: dict[str, str],
    executor_annotations: dict[str, str],
    executor_pod_template_path: str,
    executor_pod_template: str,
    output: str,
    timeout: float | None,
    driver_pod_template: str | None,
    driver_container_name: str | None,
    receipt_file,
):
    from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
    from spark_on_k8s.utils.types import NOTSET

    kwargs = {
        "image": image,
        "app_path": path,
        "namespace": namespace,
        "service_account": service_account,
        "app_name": name,
        "spark_conf": spark_conf,
        "class_name": class_name,
        "app_waiter": "log" if logs else "wait" if wait else "no_wait",
        "image_pull_policy": image_pull_policy,
        "ui_reverse_proxy": ui_reverse_proxy,
        "app_arguments": list(app_arguments) if app_arguments else NOTSET,
        "driver_resources": PodResources(
            cpu=driver_cpu,
            memory=driver_memory,
            memory_overhead=driver_memory_overhead,
        ),
        "executor_resources": PodResources(
            cpu=executor_cpu,
            memory=executor_memory,
            memory_overhead=executor_memory_overhead,
        ),
        "executor_instances": ExecutorInstances(
            min=executor_min_instances,
            max=executor_max_instances,
            initial=executor_initial_instances,
            executor_allocation_ratio=executor_allocation_ratio,
            scheduler_backlog_timeout=scheduler_backlog_timeout,
            sustained_scheduler_backlog_timeout=sustained_scheduler_backlog_timeout,
        ),
        "should_print": True,
        "secret_values": secret_env_var,
        "driver_env_vars_from_secrets": driver_env_vars_from_secrets,
        "driver_node_selector": driver_node_selector,
        "executor_node_selector": executor_node_selector,
        "driver_labels": driver_labels,
        "executor_labels": executor_labels,
        "driver_annotations": driver_annotations,
        "executor_annotations": executor_annotations,
        "executor_pod_template_path": executor_pod_template_path,
        "executor_pod_template": executor_pod_template,
        "driver_pod_template": driver_pod_template,
        "driver_container_name": driver_container_name,
    }
    execute_submission(SparkOnK8S(), kwargs, output=output, timeout=timeout, receipt_file=receipt_file)


@app_cli.command(
    cls=SparkAppCommand, help="Diagnose a Spark application using pod states and Kubernetes events."
)
@output_option
@click.option("--pod-limit", type=click.IntRange(1, 50), default=20, show_default=True)
@click.option("--event-limit", type=click.IntRange(1, 200), default=100, show_default=True)
def diagnose(app_id: str, namespace: str, output: str, pod_limit: int, event_limit: int):
    from spark_on_k8s.cli.diagnostics import display_report
    from spark_on_k8s.client import SparkOnK8S

    try:
        report = SparkOnK8S().diagnose_app(
            namespace=namespace,
            app_id=app_id,
            pod_limit=pod_limit,
            event_limit=event_limit,
        )
    except Exception as error:
        raise click.ClickException(
            "Unable to collect diagnostics; check the target and Kubernetes read access."
        ) from error
    display_report(report, output)


@app_cli.command(name="cancel", help="Request cancellation of exactly the driver in a saved run receipt.")
@click.option("--receipt", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option("--force", is_flag=True, help="Request zero grace period; does not wait for deletion.")
@output_option
def cancel(receipt, force, output):
    from pathlib import Path

    from spark_on_k8s.cli.receipts import read_receipt
    from spark_on_k8s.client import SparkOnK8S

    try:
        run = read_receipt(Path(receipt))
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    with execution("cancel", run.namespace, output) as result:
        result["run"] = run.to_dict()
        SparkOnK8S().cancel_run(run, force=force)
        result["accepted"] = True
        if output == "text":
            click.echo("Cancellation accepted; resource cleanup may still be in progress.")
