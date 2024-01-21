from __future__ import annotations

import click

from spark_on_k8s.cli.options import app_id_option, force_option, namespace_option


@click.group(name="app")
def app_cli():
    pass


class SparkAppCommand(click.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params.insert(0, app_id_option)
        self.params.insert(1, namespace_option)


@app_cli.command(cls=SparkAppCommand)
def status(app_id: str, namespace: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    print(app_manager.app_status(namespace=namespace, app_id=app_id).value)


@app_cli.command(cls=SparkAppCommand)
def logs(app_id: str, namespace: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    app_manager.stream_logs(namespace=namespace, app_id=app_id, print_logs=True)


@app_cli.command(cls=SparkAppCommand)
def kill(app_id: str, namespace: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    app_manager.kill_app(namespace=namespace, app_id=app_id)


@app_cli.command(
    cls=SparkAppCommand,
    params=[force_option],
)
def delete(app_id: str, namespace: str, force: bool):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    app_manager.delete_app(namespace=namespace, app_id=app_id, force=force)


@app_cli.command(cls=SparkAppCommand)
def wait(app_id: str, namespace: str):
    from spark_on_k8s.utils.app_manager import SparkAppManager

    app_manager = SparkAppManager()
    app_manager.wait_for_app(namespace=namespace, app_id=app_id)
    app_status = app_manager.app_status(namespace=namespace, app_id=app_id)
    print(f"App {app_id} has terminated with status {app_status.value}")
