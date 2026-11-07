from __future__ import annotations

import json

import click
import yaml

from spark_on_k8s.jobs import JobSpec, JobValidationError


@click.group(name="job", help="Validate, preview and submit versioned Spark job files.")
def job_cli():
    pass


def _load(file, profile):
    try:
        return JobSpec.from_file(file, profile=profile)
    except (JobValidationError, OSError) as error:
        raise click.ClickException(str(error)) from error


@job_cli.command(name="validate")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", help="Select environment overrides from the job file.")
def validate(file, profile):
    """Validate a job and its generated resources without accessing a cluster."""
    job = _load(file, profile)
    try:
        job.plan()
    except (ValueError, TypeError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Valid SparkJob: {job.name}")


@job_cli.command(name="plan")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", help="Select environment overrides from the job file.")
@click.option("--output", type=click.Choice(["yaml", "json"]), default="yaml", show_default=True)
def plan(file, profile, output):
    """Print a redacted offline preview. Nothing is submitted or reserved."""
    job = _load(file, profile)
    try:
        result = job.plan().to_dict()
    except (ValueError, TypeError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        json.dumps(result, indent=2) if output == "json" else yaml.safe_dump(result, sort_keys=False),
        nl=output == "json",
    )


@job_cli.command(name="submit")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", help="Select environment overrides from the job file.")
@click.option("--output", type=click.Choice(["text", "json"]), default="text", show_default=True)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0, min_open=True),
    default=None,
    help="Maximum observation time in seconds; requires a wait or log waiter.",
)
def submit(file, profile, output, timeout):
    """Submit a job using the same automation result contract as app submit."""
    from spark_on_k8s.cli.automation import execute_submission

    job = _load(file, profile)
    execute_submission(job.client(), job.to_submit_kwargs(), output=output, timeout=timeout)
