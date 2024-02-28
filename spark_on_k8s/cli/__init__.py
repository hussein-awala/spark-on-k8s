from __future__ import annotations

import click

from spark_on_k8s.cli.app import app_cli
from spark_on_k8s.cli.apps import apps_cli
from spark_on_k8s.cli.namespace import namespace_cli

try:
    from spark_on_k8s.cli.api import api_cli
except ImportError:
    api_cli = None


@click.group()
def cli():
    pass


def main():
    cli.add_command(app_cli)
    cli.add_command(apps_cli)
    cli.add_command(namespace_cli)
    if api_cli:
        cli.add_command(api_cli)
    cli(max_content_width=120)


if __name__ == "__main__":
    main()
