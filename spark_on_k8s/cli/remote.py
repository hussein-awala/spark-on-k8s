from __future__ import annotations

import json
import os
from pathlib import Path

import click

from spark_on_k8s.jobs import API_VERSION, JobSpec
from spark_on_k8s.remote import RemoteClient, RemoteError


@click.group(
    name="remote", help="Plan or submit approved jobs through an authenticated API, without local kubeconfig."
)
def remote_cli():
    pass


def _options(function):
    for option in (
        click.argument("file", type=click.Path(exists=True, dir_okay=False)),
        click.option(
            "--api-url",
            envvar="SPARK_ON_K8S_API_URL",
            required=True,
            help="HTTPS server URL, including any deployment prefix.",
        ),
        click.option(
            "--token-file",
            envvar="SPARK_ON_K8S_API_TOKEN_FILE",
            type=click.Path(exists=True, dir_okay=False),
            help="Read the bearer token from a file; otherwise use SPARK_ON_K8S_API_TOKEN.",
        ),
        click.option(
            "--timeout",
            type=click.FloatRange(min=0, min_open=True, max=300),
            default=30.0,
            show_default=True,
            help="Overall HTTP deadline in seconds; does not cancel an accepted job.",
        ),
        click.option("--output", type=click.Choice(["json", "text"]), default="json", show_default=True),
    ):
        function = option(function)
    return function


def _execute(operation, file, api_url, token_file, timeout, output):
    code = 0
    try:
        if token_file:
            with Path(token_file).open("r", encoding="ascii") as source:
                token = source.read(4097).strip()
            if len(token) > 4096:
                raise ValueError("API token file is too large.")
        else:
            token = os.environ.get("SPARK_ON_K8S_API_TOKEN", "")
        client = RemoteClient(api_url, token, timeout=timeout)
        job = JobSpec.from_file(file)
        if job.kubernetes:
            raise ValueError(
                "Remote jobs cannot supply Kubernetes credentials; the server selects its credentials."
            )
        document = {
            "apiVersion": API_VERSION,
            "kind": "SparkJob",
            "metadata": {"name": job.name},
            "spec": job.spec,
        }
        result = client.request(operation, document)
    except KeyboardInterrupt:
        code, error = (
            130,
            {"code": "interrupted", "message": "Interrupted; a submitted application was not stopped."},
        )
    except RemoteError as failure:
        code = 5 if failure.code == "timeout" else 4 if failure.status == 404 else 1
        error = {"code": failure.code, "message": str(failure)}
    except (ValueError, TypeError, OSError, UnicodeError) as failure:
        # TypeError: a YAML-only value, such as an unquoted date, that the JSON request cannot carry.
        code, error = 2, {"code": "invalid_request", "message": str(failure)}
    if code:
        result = {"schema_version": 1, "operation": operation, "error": error}
    if output == "json":
        click.echo(json.dumps(result, ensure_ascii=False))
    elif code:
        click.echo(f"Error: {result['error']['message']}", err=True)
    elif operation == "submit":
        click.echo(result["run"]["driver_pod"])
    else:
        click.echo(json.dumps(result["plan"], indent=2))
    if code:
        raise click.exceptions.Exit(code)


@remote_cli.command(name="plan")
@_options
def plan(**kwargs):
    """Request an admitted, redacted preview; no resources are created."""
    _execute("plan", **kwargs)


@remote_cli.command(name="submit")
@_options
def submit(**kwargs):
    """Submit once and return a run receipt. Requests are never retried automatically."""
    _execute("submit", **kwargs)
