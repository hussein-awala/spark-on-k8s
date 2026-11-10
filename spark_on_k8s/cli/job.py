from __future__ import annotations

import json
from pathlib import Path

import click
import yaml

from spark_on_k8s.cli.receipts import receipt_option
from spark_on_k8s.jobs import JobSpec, validation_error


@click.group(
    name="job",
    help="Validate, preview and submit versioned Spark job files; assess Spark Operator manifests.",
)
def job_cli():
    pass


def _load(file, profile):
    try:
        return JobSpec.from_file(file, profile=profile)
    except (ValueError, OSError) as error:
        # ValueError also covers a referenced template file that is not UTF-8.
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
            JobSpec.from_file(file, profile=profile).plan()
            results = [{"profile": profile, "valid": True, "error": None}]
    except (ValueError, TypeError, OSError) as error:
        results = [{"profile": profile, "valid": False, "error": validation_error(error)}]
    valid = all(item["valid"] for item in results)
    if output == "json":
        # Like other automation results, `error` is set exactly when the exit code is nonzero.
        failure = next((item["error"] for item in results if not item["valid"]), None)
        click.echo(
            json.dumps(
                {
                    "schema_version": 1,
                    "operation": "job_validate",
                    "valid": valid,
                    "profiles": results,
                    "error": failure,
                }
            )
        )
    else:
        for item in results:
            label = "base" if item["profile"] is None else item["profile"]
            click.echo(
                f"Valid SparkJob ({label})"
                if item["valid"]
                else f"Invalid SparkJob ({label}): {item['error']['message']}",
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
    except (ValueError, OSError):
        # Keep one submit document on stdout; the namespace is unknown until the job loads.
        with execution("submit", "", output) as result:
            result.update(run=None, status=None)
            raise
    execute_submission(
        job.client(), job.to_submit_kwargs(), output=output, timeout=timeout, receipt_file=receipt_file
    )


@job_cli.command(name="assess-operator")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--output", type=click.Choice(["text", "json"]), default="text", show_default=True)
@click.option(
    "--draft-dir",
    type=click.Path(exists=True, file_okay=False, writable=True),
    help="Write each valid SparkJob draft to this directory as NAME.yaml; existing files are kept.",
)
def assess_operator(file, output, draft_dir):
    """Assess Kubeflow Spark Operator manifests offline and draft SparkJob files.

    Reads sparkoperator.k8s.io/v1beta2 SparkApplication and ScheduledSparkApplication
    documents without contacting a cluster. Unsupported fields are reported, not
    translated, and do not change the exit code; unreadable input exits 1.
    """
    from spark_on_k8s.spark_operator_migration import SCHEMA_VERSION, ManifestError, assess_file

    result = {
        "schema_version": SCHEMA_VERSION,
        "operation": "assess_operator",
        "error": None,
        "migration_ready": False,
        "applications": None,
        "skipped": None,
    }
    try:
        result.update(assess_file(file))
        if draft_dir is not None:
            _write_drafts(result["applications"], Path(draft_dir), Path(file).name)
    except (ManifestError, OSError) as error:
        result["error"] = {"code": "operation_error", "message": str(error)}
    if output == "json":
        click.echo(json.dumps(result, ensure_ascii=False, default=str))
    else:
        _echo_assessment(result)
    if result["error"]:
        raise click.exceptions.Exit(1)


def _write_drafts(applications, directory: Path, source: str) -> None:
    """Check every draft destination before writing; never replace an existing file."""
    from spark_on_k8s.spark_operator_migration import ManifestError, draft_yaml

    targets = []
    for application in applications:
        if application["draft"] is None:
            continue
        path = directory / f"{application['draft']['metadata']['name']}.yaml"
        if any(path == existing for _, existing in targets):
            raise ManifestError(f"Two drafts would be written to {path}; assess the documents separately.")
        if path.exists():
            raise ManifestError(f"{path} already exists; drafts never replace existing files.")
        targets.append((application, path))
    for application, path in targets:
        with path.open("x", encoding="utf-8") as stream:
            stream.write(draft_yaml(application, source))
        application["draft_file"] = str(path)


def _echo_assessment(result) -> None:
    order = {"unsupported": 0, "approximated": 1, "mapped": 2, "ignored": 3}
    for application in result["applications"] or []:
        location = f"{application['namespace']}/" if application["namespace"] else ""
        readiness = "migration-ready" if application["migration_ready"] else "not migration-ready"
        click.echo(
            f"{application['kind']} {location}{application['name']} "
            f"(document {application['document']}): {readiness}"
        )
        click.echo("  " + ", ".join(f"{count} {status}" for status, count in application["counts"].items()))
        for finding in sorted(application["findings"], key=lambda item: order[item["status"]]):
            line = f"  {finding['status'].upper():<12} {finding['path']}"
            if finding["target"]:
                line += f" -> {finding['target']}"
            click.echo(line)
            for label in ("note", "alternative"):
                if finding[label]:
                    click.echo(f"{'':15}{label.capitalize()}: {finding[label]}")
        for note in application["notes"]:
            click.echo(f"  Note: {note}")
        if application["draft_file"]:
            click.echo(f"  Draft written to {application['draft_file']}; validate it with `job validate`.")
        elif application["draft"] is not None:
            click.echo("  Draft: valid offline; write it with --draft-dir.")
        else:
            click.echo(f"  Draft: not produced: {application['draft_error']}")
    for item in result["skipped"] or []:
        click.echo(
            f"Skipped document {item['document']} ({item['api_version']} {item['kind']}): {item['reason']}"
        )
    if result["error"]:
        click.echo(f"Error: {result['error']['message']}", err=True)
