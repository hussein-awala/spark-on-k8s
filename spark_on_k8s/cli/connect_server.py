from __future__ import annotations

import json
from pathlib import Path

import click

from spark_on_k8s.cli.automation import execution, output_option, report_warnings, validate_timeout
from spark_on_k8s.cli.options import validate_dictionary_option
from spark_on_k8s.cli.receipts import receipt_writer
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.connect import ConnectProfile
from spark_on_k8s.connect_server import ConnectServerConfig, ConnectServerCreateError, ConnectServerHandle
from spark_on_k8s.k8s.sync_client import KubernetesClientManager


def _cluster_options(function):
    function = click.option(
        "--kubeconfig",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        help="Kubernetes configuration file; does not change the current context.",
    )(function)
    return click.option("--context", help="Kubernetes context to use.")(function)


def _timeout(default):
    return click.option(
        "--timeout",
        type=click.FloatRange(min=0, min_open=True),
        callback=validate_timeout,
        default=default,
        show_default=True,
        help="Total seconds for this operation. A readiness timeout retains the server.",
    )


def _client(kubeconfig, context):
    options = {}
    if kubeconfig is not None:
        options["config_file"] = str(kubeconfig)
    if context is not None:
        options["context"] = context
    if kubeconfig is not None or context is not None:
        options["in_cluster"] = False
    return SparkOnK8S(k8s_client_manager=KubernetesClientManager(**options))


def _read_server_receipt(path: Path) -> ConnectServerHandle:
    with path.open("rb") as stream:
        data = stream.read(65537)
    if len(data) > 65536:
        raise ValueError("Connect server receipt exceeds 64 KiB")
    try:
        document = json.loads(data)
    except (ValueError, UnicodeError):
        raise ValueError("Invalid Connect server receipt JSON") from None
    if (
        not isinstance(document, dict)
        or set(document) != {"schema_version", "server"}
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
    ):
        raise ValueError("Expected a version 1 Connect server receipt")
    return ConnectServerHandle.from_dict(document["server"])


@click.group(name="server", help="Create and manage a private Spark Connect server on Kubernetes.")
def server_cli():
    pass


@server_cli.command(
    name="create", help="Create a server and save its identity; query readiness is a separate step."
)
@click.option("--image", required=True, help="Spark 4 image containing the Connect server JAR.")
@click.option(
    "--server-jar", required=True, help="Exact local:/// path to the Connect server JAR in the image."
)
@click.option("--namespace", required=True, help="Existing Kubernetes namespace.")
@click.option(
    "--service-account", required=True, help="Existing driver service account with executor permissions."
)
@click.option("--name", default="spark-connect", show_default=True)
@click.option("--port", type=click.IntRange(1, 65535), default=15002, show_default=True)
@click.option(
    "--image-pull-policy",
    type=click.Choice(["Always", "IfNotPresent", "Never"]),
    default="IfNotPresent",
    show_default=True,
)
@click.option("--driver-cpu", type=click.IntRange(min=1), default=1, show_default=True)
@click.option(
    "--driver-memory", type=click.IntRange(min=1), default=1024, show_default=True, help="Driver heap in MiB."
)
@click.option("--driver-memory-overhead", type=click.IntRange(min=0), default=512, show_default=True)
@click.option("--executor-cpu", type=click.IntRange(min=1), default=1, show_default=True)
@click.option(
    "--executor-memory",
    type=click.IntRange(min=1),
    default=1024,
    show_default=True,
    help="Executor heap in MiB.",
)
@click.option("--executor-memory-overhead", type=click.IntRange(min=0), default=512, show_default=True)
@click.option("--executors", type=click.IntRange(min=1), default=2, show_default=True)
@click.option(
    "--conf",
    "spark_conf",
    multiple=True,
    callback=validate_dictionary_option,
    help="Additional Spark key=value setting; managed identity/network settings cannot be overridden.",
)
@click.option("--package", "packages", multiple=True, help="Additional Maven package coordinate.")
@click.option(
    "--receipt-file",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="New file for the server identity. Existing files are never replaced.",
)
@_cluster_options
@output_option
def create(
    image,
    server_jar,
    namespace,
    service_account,
    name,
    port,
    image_pull_policy,
    driver_cpu,
    driver_memory,
    driver_memory_overhead,
    executor_cpu,
    executor_memory,
    executor_memory_overhead,
    executors,
    spark_conf,
    packages,
    receipt_file,
    kubeconfig,
    context,
    output,
):
    with execution("connect_server_create", namespace, output) as result:
        result.update(server=None, endpoint=None, query_ready=False)
        config = ConnectServerConfig(
            image=image,
            server_jar=server_jar,
            namespace=namespace,
            service_account=service_account,
            app_name=name,
            port=port,
            image_pull_policy=image_pull_policy,
            driver_resources=PodResources(
                cpu=driver_cpu, memory=driver_memory, memory_overhead=driver_memory_overhead
            ),
            executor_resources=PodResources(
                cpu=executor_cpu, memory=executor_memory, memory_overhead=executor_memory_overhead
            ),
            executor_instances=ExecutorInstances(initial=executors),
            spark_conf=spark_conf,
            packages=list(packages),
        )
        with receipt_writer(receipt_file, field="server") as save:
            spark_client = _client(kubeconfig, context)
            try:
                handle = spark_client.create_connect_server(config)
            except KeyboardInterrupt as error:
                result["phase"] = getattr(error, "phase", None) or spark_client.last_submission_phase
                result["cleanup"] = getattr(error, "cleanup", None) or {
                    "status": "not_attempted",
                    "reason": "Interruption does not initiate cleanup",
                }
                run = getattr(error, "run", None) or spark_client.last_submitted_run
                if run is not None:
                    result["partial_run"] = run.to_dict()
                    if output == "text":
                        click.echo(
                            "Retain this partial identity for inspection: "
                            f"{json.dumps(result['partial_run'])}",
                            err=True,
                        )
                        click.echo(f"Cleanup: {json.dumps(result['cleanup'])}", err=True)
                handle = getattr(error, "handle", None)
                if handle is not None:
                    result["server"] = handle.to_dict()
                    try:
                        save(handle)
                    except OSError:
                        result["receipt_error"] = {
                            "code": "receipt_write_error",
                            "message": "Partial receipt could not be saved; retain the returned identity.",
                        }
                raise
            except ConnectServerCreateError as error:
                result.update(phase=error.phase, cleanup=error.cleanup)
                if error.run is not None:
                    result["partial_run"] = error.run.to_dict()
                    if output == "text":
                        click.echo(
                            f"Inspect driver {error.run.driver_pod} in {error.run.namespace}; "
                            f"creation phase: {error.phase}.",
                            err=True,
                        )
                        click.echo(f"Partial run identity: {json.dumps(error.run.to_dict())}", err=True)
                        click.echo(f"Cleanup: {json.dumps(error.cleanup)}", err=True)
                if error.handle is not None:
                    result["server"] = error.handle.to_dict()
                    try:
                        save(error.handle)
                    except OSError:
                        result["receipt_error"] = {
                            "code": "receipt_write_error",
                            "message": "Partial receipt could not be saved; retain the returned identity.",
                        }
                raise
            result.update(server=handle.to_dict(), endpoint=handle.endpoint)
            try:
                save(handle)
            except OSError:
                if output == "text":
                    click.echo(
                        "Server resources were created. Retain this identity for cleanup: "
                        f"{json.dumps(handle.to_dict())}",
                        err=True,
                    )
                raise
        if output == "text":
            click.echo(f"Created server resources in {namespace}; query readiness has not been checked.")
            click.echo(f"Private endpoint: {handle.endpoint}")
            click.echo(f"Receipt: {receipt_file}")
            click.echo(
                f"For local access, forward Service {handle.service_name} in {namespace}, "
                f"port {handle.port}, then run connect server wait with a reachable profile."
            )


@server_cli.command(
    name="status", help="Observe the recorded driver and Services without claiming query readiness."
)
@click.argument("receipt", type=click.Path(dir_okay=False, path_type=Path))
@_cluster_options
@_timeout(10)
@output_option
def status(receipt, kubeconfig, context, timeout, output):
    with execution("connect_server_status", "", output) as result:
        handle = _read_server_receipt(receipt)
        result.update(namespace=handle.run.namespace, server=handle.to_dict())
        result["observation"] = _client(kubeconfig, context).connect_server_status(handle, timeout=timeout)
        if output == "text":
            click.echo(json.dumps(result["observation"], indent=2))


def _age(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    if days:
        return f"{days}d{hours}h"
    if hours:
        return f"{hours}h{minutes}m"
    return f"{minutes}m" if minutes else f"{seconds}s"


@server_cli.command(
    name="list",
    help="List labeled Connect server drivers in a namespace, oldest first; read-only, no receipt needed.",
)
@click.option("--namespace", required=True, help="Kubernetes namespace to inspect.")
@_cluster_options
@_timeout(10)
@output_option
def list_servers(namespace, kubeconfig, context, timeout, output):
    with execution("connect_server_list", namespace, output) as result:
        result["items"] = None
        result["items"] = _client(kubeconfig, context).list_connect_servers(namespace, timeout=timeout)
        if output == "text" and not result["items"]:
            click.echo(f"No Spark Connect server drivers created by spark-on-k8s were found in {namespace}.")
        elif output == "text":
            rows = [("APP ID", "STATUS", "AGE", "CREATED", "SPARK", "IMAGE")] + [
                (
                    item["app_id"],
                    item["status"],
                    _age(item["age_seconds"]),
                    item["created_at"] or "-",
                    item["spark_version"] or "-",
                    item["image"] or "-",
                )
                for item in result["items"]
            ]
            widths = [max(len(row[column]) for row in rows) for column in range(len(rows[0]))]
            for row in rows:
                click.echo(
                    "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip()
                )


@server_cli.command(
    name="wait", help="Wait for a successful query from the server identified by this receipt."
)
@click.argument("receipt", type=click.Path(dir_okay=False, path_type=Path))
@click.option("--profile-file", type=click.Path(dir_okay=False, path_type=Path), required=True)
@click.option("--profile", "profile_name", default="default", show_default=True)
@_cluster_options
@_timeout(60)
@output_option
def wait(receipt, profile_file, profile_name, kubeconfig, context, timeout, output):
    with execution("connect_server_wait", "", output) as result:
        result["warnings"] = []
        handle = _read_server_receipt(receipt)
        result.update(namespace=handle.run.namespace, server=handle.to_dict(), query_ready=False)
        profile = ConnectProfile.from_file(profile_file, profile=profile_name)
        result["observation"] = _client(kubeconfig, context).wait_connect_server_ready(
            handle, profile=profile, timeout=timeout
        )
        result["query_ready"] = True
        result["warnings"] = result["observation"].get("warnings", [])
        if output == "text":
            click.echo(f"Server {handle.run.app_id} accepted a query through the selected profile.")
        report_warnings(result["warnings"], output)


@server_cli.command(
    name="delete", help="Request shutdown and cleanup of exactly the recorded server resources."
)
@click.argument("receipt", type=click.Path(dir_okay=False, path_type=Path))
@_cluster_options
@_timeout(10)
@output_option
def delete(receipt, kubeconfig, context, timeout, output):
    with execution("connect_server_delete", "", output) as result:
        handle = _read_server_receipt(receipt)
        result.update(namespace=handle.run.namespace, server=handle.to_dict())
        result["deletion"] = _client(kubeconfig, context).delete_connect_server(handle, timeout=timeout)
        if output == "text":
            if result["deletion"]["status"] == "absent":
                click.echo("The recorded server resources are already absent.")
            else:
                click.echo(
                    "Deletion requested for the recorded server. Kubernetes cleanup may still be in progress."
                )
