from __future__ import annotations

import click

from spark_on_k8s.cli.app import app_cli
from spark_on_k8s.cli.apps import apps_cli
from spark_on_k8s.cli.connect import connect_cli
from spark_on_k8s.cli.diagnostics import doctor
from spark_on_k8s.cli.job import job_cli
from spark_on_k8s.cli.namespace import namespace_cli
from spark_on_k8s.cli.remote import remote_cli
from spark_on_k8s.cli.sql import sql_cli

API_EXTRA_MESSAGE = "The API requires the 'api' extra: pip install 'spark-on-k8s[api]'"

try:
    from spark_on_k8s.cli.api import api_cli
except ImportError:
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


@click.group()
def cli():
    pass


def main():
    cli.add_command(app_cli)
    cli.add_command(apps_cli)
    cli.add_command(namespace_cli)
    cli.add_command(job_cli)
    cli.add_command(sql_cli)
    cli.add_command(connect_cli)
    cli.add_command(remote_cli)
    cli.add_command(doctor)
    cli.add_command(api_cli)
    cli(max_content_width=120)


if __name__ == "__main__":
    main()
