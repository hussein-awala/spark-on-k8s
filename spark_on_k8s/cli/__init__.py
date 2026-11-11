from __future__ import annotations

import click

from spark_on_k8s.utils.env import ConfigurationError

API_EXTRA_MESSAGE = "The API requires the 'api' extra: pip install 'spark-on-k8s[api]'"


def _missing_api_cli():
    # Keep the command discoverable so a base installation explains the missing extra.
    @click.group(name="api", help="Manage the spark-on-k8s API (requires the 'api' extra).")
    def api_cli():
        pass

    @api_cli.command(
        help="Start the spark-on-k8s API.",
        context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    )
    def start():
        raise click.ClickException(API_EXTRA_MESSAGE)

    return api_cli


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(package_name="spark-on-k8s")
def cli():
    """Submit, watch and manage Spark applications on Kubernetes without an operator."""


def _commands():
    # Imported here: the command modules read SPARK_ON_K8S_* variables when they load.
    from spark_on_k8s.cli.app import app_cli
    from spark_on_k8s.cli.apps import apps_cli
    from spark_on_k8s.cli.connect import connect_cli
    from spark_on_k8s.cli.diagnostics import doctor
    from spark_on_k8s.cli.job import job_cli
    from spark_on_k8s.cli.namespace import namespace_cli
    from spark_on_k8s.cli.remote import remote_cli
    from spark_on_k8s.cli.sql import sql_cli

    try:
        from spark_on_k8s.cli.api import api_cli
    except ImportError:
        api_cli = _missing_api_cli()
    return app_cli, apps_cli, namespace_cli, job_cli, sql_cli, connect_cli, remote_cli, doctor, api_cli


def main():
    try:
        commands = _commands()
    except ConfigurationError as error:
        # A malformed variable would otherwise end every command, --help included, in a traceback.
        click.echo(f"Error: {error}", err=True)
        raise SystemExit(1) from None
    for command in commands:
        cli.add_command(command)
    cli(max_content_width=120)


if __name__ == "__main__":
    main()
