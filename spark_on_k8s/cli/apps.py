from __future__ import annotations

import click

from spark_on_k8s.cli.automation import execution, output_option
from spark_on_k8s.cli.options import namespace_option


@click.group(name="apps", help="Manage Spark applications in a namespace.")
def apps_cli():
    pass


class SparkAppsCommand(click.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.append(namespace_option)


@apps_cli.command(cls=SparkAppsCommand, help="List all Spark applications in a namespace.")
@output_option
def list(namespace: str, output: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    with execution("list", namespace, output) as result:
        result["items"] = None
        apps = SparkAppManager().list_apps(namespace=namespace)
        result["items"] = [{"app_id": app_id, "namespace": namespace} for app_id in apps]
        if output == "text":
            for app_id in apps:
                click.echo(app_id)


def _item_error(error: Exception) -> dict[str, str]:
    from kubernetes.client.exceptions import ApiException

    if isinstance(error, ApiException):
        # Do not serialize Kubernetes response bodies, as the shared execution contract does.
        return {"code": "api_error", "message": f"Kubernetes API {error.status}: {error.reason or 'failed'}"}
    return {"code": "operation_error", "message": str(error) or type(error).__name__}


@apps_cli.command(
    cls=SparkAppsCommand,
    help=(
        "Find drivers created by spark-on-k8s that finished (or, with --include-active, are still "
        "active) and are older than a threshold. Only reports them unless --delete is given; deleting "
        "a driver lets Kubernetes garbage-collect its executors, Services, ConfigMaps and Secrets."
    ),
)
@click.option(
    "--older-than",
    type=click.IntRange(min=0),
    default=0,
    show_default=True,
    help="Select only drivers created at least this many seconds ago.",
)
@click.option(
    "--include-active",
    is_flag=True,
    help=(
        "Also select Pending, Running and Unknown drivers; deleting them stops those applications. "
        "Requires a positive --older-than."
    ),
)
@click.option(
    "--delete",
    is_flag=True,
    help="Request deletion of the selected drivers. Without it, nothing is changed.",
)
@output_option
def prune(namespace: str, older_than: int, include_active: bool, delete: bool, output: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    if include_active and older_than == 0:
        raise click.UsageError("--include-active requires a positive --older-than")
    with execution("prune", namespace, output) as result:
        result.update(
            dry_run=not delete,
            criteria={"older_than": older_than, "include_active": include_active},
            items=None,
            excluded=None,
        )
        manager = SparkAppManager()
        selection = manager.find_prunable_drivers(
            namespace=namespace, older_than=older_than, include_active=include_active
        )
        result["excluded"] = selection["excluded"]
        # Fill items as work progresses so an interruption still reports earlier requests.
        result["items"] = []
        failures = 0
        for item in selection["items"]:
            entry = {**item, "action": "would_delete" if not delete else None, "error": None}
            result["items"].append(entry)
            if delete:
                try:
                    entry["action"] = manager.delete_prunable_driver(namespace=namespace, item=item)
                except Exception as error:
                    failures += 1
                    entry.update(action="failed", error=_item_error(error))
            if output == "text":
                description = f"{entry['driver_pod']} (application {entry['app_id']}, {entry['phase']}"
                description += f"/{entry['reason']}" if entry["reason"] else ""
                description += f", age {entry['age_seconds']}s)"
                messages = {
                    "would_delete": f"Would delete {description}",
                    "deletion_requested": f"Deletion requested for {description}",
                    "absent": f"Already absent: {description}",
                    "replaced": f"Not deleted, a different pod now has this name: {description}",
                    "failed": f"Failed to delete {description}: {(entry['error'] or {}).get('message')}",
                }
                click.echo(messages[entry["action"]], err=entry["action"] == "failed")
        if output == "text":
            labels = {
                "active": "still active (see --include-active)",
                "recent": "newer than --older-than",
                "deleting": "already being deleted",
                "unverified": "not verifiably created by spark-on-k8s",
            }
            skipped = ", ".join(
                f"{count} {labels[reason]}" for reason, count in result["excluded"].items() if count
            )
            if skipped:
                click.echo(f"Not selected: {skipped}.")
            if not delete and result["items"]:
                click.echo(f"{len(result['items'])} driver(s) selected; nothing was deleted (use --delete).")
            elif not result["items"]:
                click.echo("No drivers matched.")
        if failures:
            raise RuntimeError(
                f"Deletion failed for {failures} of {len(result['items'])} selected drivers; "
                "the other requests were sent. Inspect the failed items before retrying."
            )
