from __future__ import annotations

import json

import click
import yaml

from spark_on_k8s.cli.options import namespace_option, service_account_option


def display_report(report, output: str) -> None:
    if output == "json":
        click.echo(json.dumps(report.to_dict()))
        return
    click.echo(f"Namespace: {report.namespace}")
    for finding in report.findings:
        click.echo(f"[{finding.severity}] {finding.resource}: {finding.summary}")
        click.echo(f"  Evidence: {finding.evidence}")
        click.echo(f"  Next: {finding.remediation}")
    if not report.findings:
        click.echo("No specific problems observed in the collected evidence.")
    if hasattr(report, "driver_permissions_verified"):
        click.echo(
            "Permission checks apply to the caller; driver service-account permissions are not verified."
        )


@click.command(
    params=[namespace_option, service_account_option], help="Read-only cluster and service-account checks."
)
@click.option("--output", type=click.Choice(["text", "json"]), default="text", show_default=True)
@click.option(
    "--emit-rbac",
    is_flag=True,
    help="Print a namespace-scoped driver Role/RoleBinding YAML without contacting Kubernetes.",
)
def doctor(namespace: str, service_account: str, output: str, emit_rbac: bool):
    from spark_on_k8s.client import SparkOnK8S
    from spark_on_k8s.doctor import driver_rbac

    try:
        if emit_rbac:
            click.echo(yaml.safe_dump_all(driver_rbac(namespace, service_account), sort_keys=False), nl=False)
            return
        report = SparkOnK8S().doctor(namespace=namespace, service_account=service_account)
    except Exception as error:
        raise click.ClickException(
            "Unable to check cluster prerequisites; check credentials and target names."
        ) from error
    display_report(report, output)
