from __future__ import annotations

import click

from spark_on_k8s.cli.options import namespace_option


@click.group(name="apps")
def apps_cli():
    pass


class SparkAppsCommand(click.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.append(namespace_option)


@apps_cli.command(cls=SparkAppsCommand)
def list(namespace: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    apps = app_manager.list_apps(namespace=namespace)
    for app in apps:
        print(app)
