from __future__ import annotations

import click

from spark_on_k8s.cli.automation import execution, report_warnings, validate_timeout
from spark_on_k8s.cli.connect_server import server_cli
from spark_on_k8s.connect import ConnectProfile


@click.group(name="connect", help="Use Spark Connect profiles or create a private Kubernetes server.")
def connect_cli():
    pass


@connect_cli.command(name="check")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", default="default", show_default=True)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0, min_open=True),
    callback=validate_timeout,
    default=15,
    show_default=True,
)
@click.option("--output", type=click.Choice(["text", "json"]), default="text", show_default=True)
def check(file, profile, timeout, output):
    """Run a tiny remote query, then close only the check's session."""
    # The check uses no Kubernetes namespace; the shared contract maps failures to exit codes.
    with execution("connect_check", "", output) as result:
        result.update(status="error", spark_version=None, client_version=None, warnings=[])
        probe = ConnectProfile.from_file(file, profile=profile).probe(timeout=timeout)
        result.update(
            status=probe["status"],
            spark_version=probe.get("spark_version"),
            client_version=probe.get("client_version"),
            warnings=probe.get("warnings", []),
        )
        if output == "text":
            click.echo(
                f"Connected to Spark {result['spark_version']} with client "
                f"{result['client_version'] or 'unknown'}; check session closed."
            )
        report_warnings(result["warnings"], output)


connect_cli.add_command(server_cli)
