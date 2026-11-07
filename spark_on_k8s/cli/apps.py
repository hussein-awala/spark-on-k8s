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
