from __future__ import annotations

import json

import click
import yaml

from spark_on_k8s.cli.receipts import receipt_option
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
@click.option("--all-profiles", is_flag=True, help="Check the base and every named profile offline.")
@click.option("--output", type=click.Choice(["text", "json"]), default="text")
def validate(file, profile, all_profiles, output):
    """Validate a job and its generated resources without accessing a cluster."""
    if all_profiles and profile is not None:
        raise click.UsageError("Choose --profile or --all-profiles, not both")
    try:
        if all_profiles:
            results = JobSpec.validate_profiles(file)
        else:
            job = _load(file, profile)
            job.plan()
            results = [{"profile": profile, "valid": True, "error": None}]
    except (ValueError, TypeError, OSError, click.ClickException) as error:
        results = [{"profile": profile, "valid": False, "error": str(error)}]
    valid = all(item["valid"] for item in results)
    if output == "json":
        click.echo(json.dumps({"schema_version": 1, "valid": valid, "profiles": results}))
    else:
        for item in results:
            label = "base" if item["profile"] is None else item["profile"]
            click.echo(
                f"Valid SparkJob ({label})"
                if item["valid"]
                else f"Invalid SparkJob ({label}): {item['error']}",
                err=not item["valid"],
            )
    if not valid:
        raise click.exceptions.Exit(1)


@job_cli.command(name="plan")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--profile", help="Select environment overrides from the job file.")
@click.option("--output", type=click.Choice(["yaml", "json"]), default="yaml", show_default=True)
@click.option(
    "--max-cpu",
    type=click.FloatRange(min=0, min_open=True),
    help="Fail if peak CPU requests exceed this budget or are unknown.",
)
@click.option(
    "--max-memory-mib",
    type=click.FloatRange(min=0, min_open=True),
    help="Fail if peak memory requests exceed this MiB budget or are unknown.",
)
def plan(file, profile, output, max_cpu, max_memory_mib):
    """Print a redacted offline preview. Nothing is submitted or reserved."""
    job = _load(file, profile)
    try:
        result = job.plan().to_dict()
        if max_cpu is not None or max_memory_mib is not None:
            from spark_on_k8s.resource_plan import check_budget

            result["budget"] = check_budget(
                result["capacity"], max_cpu=max_cpu, max_memory_mib=max_memory_mib
            )
    except (ValueError, TypeError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        json.dumps(result, indent=2) if output == "json" else yaml.safe_dump(result, sort_keys=False),
        nl=output == "json",
    )

    if result.get("budget", {}).get("passed") is False:
        raise click.exceptions.Exit(1)


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
@receipt_option
def submit(file, profile, output, timeout, receipt_file):
    """Submit a job using the same automation result contract as app submit."""
    from spark_on_k8s.cli.automation import execute_submission, execution

    try:
        job = JobSpec.from_file(file, profile=profile)
    except (JobValidationError, OSError):
        # Keep one submit document on stdout; the namespace is unknown until the job loads.
        with execution("submit", "", output) as result:
            result.update(run=None, status=None)
            raise
    execute_submission(
        job.client(), job.to_submit_kwargs(), output=output, timeout=timeout, receipt_file=receipt_file
    )
