from __future__ import annotations

import json

import click

from spark_on_k8s.cli.connect_server import server_cli
from spark_on_k8s.connect import ConnectError, ConnectProfile


@click.group(name="connect", help="Use Spark Connect profiles or create a private Kubernetes server.")
def connect_cli():
    pass


@connect_cli.command(name="check")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", default="default", show_default=True)
@click.option("--timeout", type=click.FloatRange(min=0, min_open=True), default=15, show_default=True)
@click.option("--output", type=click.Choice(["text", "json"]), default="text", show_default=True)
def check(file, profile, timeout, output):
    """Run a tiny remote query, then close only the check's session."""
    code = 0
    try:
        result = ConnectProfile.from_file(file, profile=profile).probe(timeout=timeout)
    except TimeoutError as error:
        result = {"schema_version": 1, "status": "error", "error": str(error)}
        code = 5
    except (ConnectError, OSError) as error:
        result = {"schema_version": 1, "status": "error", "error": str(error)}
        code = 1
    if output == "json":
        click.echo(json.dumps(result))
    elif code:
        click.echo(result["error"], err=True)
    else:
        click.echo(f"Connected to Spark {result['spark_version']}; check session closed.")
    if code:
        raise click.exceptions.Exit(code)


connect_cli.add_command(server_cli)
