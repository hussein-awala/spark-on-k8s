"""Read-only preflight for a managed dbt profile.

The check selects and validates a dbt target with the code the adapter uses,
then reads Kubernetes metadata and asks SelfSubjectAccessReview questions. It
creates no Spark server, Pod, Service or other persistent resource: access
reviews are evaluated and never stored. Passing does not prove that a dbt
invocation will succeed.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import math
import os
import shutil
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from spark_on_k8s_dbt.api import BoundedKubernetesClientManager
from spark_on_k8s_dbt.progress import TRANSPORTS

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA_VERSION = 1

# Requests the invoking identity makes; see lifecycle.py, thrift_server.py and
# spark_on_k8s.connect_server. Managed servers are submitted without Secrets or
# ConfigMaps; executor ConfigMaps are created by the driver's service account.
REQUIRED_ACCESS = (
    ("get", "namespaces"),
    ("get", "serviceaccounts"),
    ("create", "pods"),
    ("get", "pods"),
    ("list", "pods"),
    ("delete", "pods"),
    ("create", "services"),
    ("get", "services"),
    ("list", "services"),
    ("delete", "services"),
    ("list", "configmaps"),
)
# A local client reaches the private endpoint through kubectl port-forward.
TUNNEL_ACCESS = ("create", "pods/portforward")
CLIENT_MODULES = {
    "connect": ("pyspark.sql.connect.session",),
    "thrift": ("pyhive.hive", "thrift_sasl", "TCLIService.ttypes"),
}
CLIENT_DISTRIBUTIONS = {"connect": ("pyspark-client",), "thrift": ("PyHive", "thrift")}
CAVEAT = (
    "A passing check does not prove that dbt will succeed: image pulls, quotas, admission "
    "policy, scheduling, the driver service account's own permissions, network policy, and "
    "the warehouse and metastore are exercised only by a real invocation."
)


def permission_id(verb: str, resource: str) -> str:
    return f"permission.{verb}.{resource}"


CHECK_IDS = (
    "profile",
    "compute",
    "client_libraries",
    "kubectl",
    "kubernetes_config",
    "namespace",
    "service_account",
    *(permission_id(verb, resource) for verb, resource in (*REQUIRED_ACCESS, TUNNEL_ACCESS)),
)


@dataclass(frozen=True)
class Check:
    id: str
    status: Literal["pass", "fail", "skip"]
    message: str
    remediation: str | None = None


@dataclass
class CheckReport:
    """Versioned result; every ID in ``CHECK_IDS`` appears once, in that order."""

    profile: str | None = None
    target: str | None = None
    method: str | None = None
    namespace: str | None = None
    checks: list[Check] = field(default_factory=list)
    error: dict[str, str] | None = None

    @property
    def status(self) -> str:
        return "fail" if self.error or any(check.status == "fail" for check in self.checks) else "pass"

    def add(self, check_id: str, status: str, message: str, remediation: str | None = None) -> None:
        self.checks.append(Check(check_id, status, message, remediation))

    def skip_remaining(self, reason: str) -> None:
        """Record every check not yet run as skipped, keeping the canonical order."""
        recorded = {check.id for check in self.checks}
        for check_id in CHECK_IDS:
            if check_id not in recorded:
                self.add(check_id, "skip", f"Skipped: {reason}.")
        self.checks.sort(key=lambda check: CHECK_IDS.index(check.id))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "operation": "dbt_check",
            "status": self.status,
            "profile": self.profile,
            "target": self.target,
            "method": self.method,
            "namespace": self.namespace,
            "checks": [asdict(check) for check in self.checks],
            "error": self.error,
        }


def _message(error: BaseException) -> str:
    # dbt exceptions keep their secret-scrubbed text in msg; str() adds a heading.
    message = getattr(error, "msg", None)
    return " ".join((message if isinstance(message, str) else str(error) or type(error).__name__).split())


@contextmanager
def dbt_events_on_stderr():
    """Show dbt's own profile warnings on stderr, keeping stdout for the check result."""
    from dbt_common.events.base_types import EventLevel
    from dbt_common.events.event_manager import EventManager
    from dbt_common.events.event_manager_client import ctx_set_event_manager, get_event_manager
    from dbt_common.events.functions import get_stdout_config
    from dbt_common.events.logger import LineFormat

    previous = get_event_manager()
    manager = EventManager()
    config = get_stdout_config(LineFormat.PlainText, False, EventLevel.INFO, False)
    config.output_stream = sys.stderr
    manager.add_logger(config)
    ctx_set_event_manager(manager)
    try:
        yield
    finally:
        ctx_set_event_manager(previous)


def load_profile(
    project_dir: Path, profiles_dir: Path, profile_name: str | None, target: str | None, cli_vars
):
    """Select, render and validate a target as dbt's load_profile does, with explicit directories."""
    from argparse import Namespace

    from dbt import flags
    from dbt.config.profile import Profile, read_profile
    from dbt.config.project import load_raw_project
    from dbt.config.renderer import ProfileRenderer
    from dbt_common.context import set_invocation_context

    if not (profiles_dir / "profiles.yml").is_file():
        raise FileNotFoundError(f"No profiles.yml in {profiles_dir}; pass --profiles-dir")
    previous = flags.get_flags()
    # Do what dbt's CLI preflight does for profile loading, without its logging
    # and usage-tracking setup: Profile reads global flags and env_var() reads
    # the invocation environment. Restore the previous flags afterwards.
    set_invocation_context(os.environ)
    flags.set_from_args(Namespace(profiles_dir=str(profiles_dir), project_dir=str(project_dir)), None)
    try:
        renderer = ProfileRenderer(cli_vars)
        project_profile = None
        if profile_name is None:
            project_profile = renderer.render_value(load_raw_project(str(project_dir)).get("profile"))
        profile_name = Profile.pick_profile_name(profile_name, project_profile)
        return Profile.from_raw_profiles(read_profile(str(profiles_dir)), profile_name, renderer, target)
    finally:
        flags.set_flags(previous)


def _check_profile(report, project_dir, profiles_dir, profile_name, target, cli_vars):
    try:
        with dbt_events_on_stderr():
            profile = load_profile(project_dir, profiles_dir, profile_name, target, cli_vars)
    except Exception as error:
        report.add(
            "profile",
            "fail",
            _message(error),
            "Pass --project-dir/--profiles-dir/--profile/--target as you pass them to dbt, "
            "and fix the selected output.",
        )
        return None
    report.profile, report.target = profile.profile_name, profile.target_name
    credentials = profile.credentials
    if credentials.type != "spark_on_k8s":
        report.add(
            "profile",
            "fail",
            f"Target {profile.target_name} uses adapter type {credentials.type}, not spark_on_k8s.",
            "Select a spark_on_k8s output with --target.",
        )
        return None
    report.method = credentials.method
    report.add(
        "profile",
        "pass",
        f"Target {profile.target_name} of profile {profile.profile_name} is a valid spark_on_k8s "
        f"output using {TRANSPORTS[credentials.method]}.",
    )
    return credentials


def _check_compute(report, credentials):
    from spark_on_k8s_dbt.config import ComputeConfig

    try:
        config = ComputeConfig.parse(credentials.method, credentials.compute)
        if credentials.method == "thrift":
            from spark_on_k8s_dbt.thrift_server import ThriftServer

            # Validate managed Thrift settings as server creation does, without a client.
            ThriftServer(None, config.server, lambda state: None)
    except Exception as error:
        report.add(
            "compute",
            "fail",
            f"Invalid compute settings: {_message(error)}",
            "Fix the output's compute mapping; see the managed dbt guide for accepted fields.",
        )
        return None
    report.namespace = config.server.namespace
    report.add(
        "compute",
        "pass",
        f"Managed {TRANSPORTS[credentials.method]} settings are valid: image {config.server.image}, "
        f"namespace {config.server.namespace}, service account {config.server.service_account}.",
    )
    return config


def _check_client_libraries(report, method):
    failures = []
    for module in CLIENT_MODULES[method]:
        try:
            importlib.import_module(module)
        except Exception as error:
            failures.append(f"{module} ({_message(error)})")
    if failures:
        report.add(
            "client_libraries",
            "fail",
            f"{TRANSPORTS[method]} client libraries cannot be imported: {'; '.join(failures)}.",
            "Install dbt-spark-on-k8s with its declared dependencies in this Python environment.",
        )
        return
    versions = []
    for name in CLIENT_DISTRIBUTIONS[method]:
        try:
            versions.append(f"{name} {importlib.metadata.version(name)}")
        except importlib.metadata.PackageNotFoundError:
            versions.append(f"{name} (version unknown)")
    report.add("client_libraries", "pass", f"{TRANSPORTS[method]} client importable: {', '.join(versions)}.")


def _check_kubectl(report, config):
    if config.in_cluster:
        report.add("kubectl", "skip", "Not used: an in-cluster client connects to the private Service.")
        return
    path = shutil.which("kubectl")
    if path is None:
        report.add(
            "kubectl",
            "fail",
            "kubectl is not on PATH; a local client opens its private tunnel with kubectl port-forward.",
            "Install kubectl or add it to PATH for the process that runs spark-on-k8s-dbt.",
        )
    else:
        report.add("kubectl", "pass", f"kubectl found at {path}.")


def _failure(error: BaseException) -> tuple[str, bool]:
    """Describe a failed request; True when later requests cannot succeed either."""
    status = _status(error)
    if status:
        # Never include response bodies.
        return f"Kubernetes API {status}: {error.reason or 'request failed'}", status == 401
    if isinstance(error, TimeoutError):
        return "The check's Kubernetes time budget expired.", True
    reason = getattr(error, "reason", None) or error
    return (
        f"Kubernetes API unreachable: {_message(reason) if isinstance(reason, BaseException) else reason}",
        True,
    )


def _review(api, namespace, verb, resource, name):
    from kubernetes import client as k8s

    resource, _, subresource = resource.partition("/")
    review = api.create_self_subject_access_review(
        body=k8s.V1SelfSubjectAccessReview(
            spec=k8s.V1SelfSubjectAccessReviewSpec(
                resource_attributes=k8s.V1ResourceAttributes(
                    namespace=namespace,
                    group="",
                    resource=resource,
                    subresource=subresource or None,
                    verb=verb,
                    name=name,
                )
            )
        )
    )
    return review.status


def _check_cluster(report, config, timeout):
    from kubernetes import client as k8s

    namespace = config.server.namespace
    deadline = time.monotonic() + timeout
    manager = BoundedKubernetesClientManager(
        deadline=lambda: deadline,
        config_file=config.kube_config_file,
        context=config.kube_context,
        in_cluster=config.in_cluster,
        client_configuration=None,
    )
    try:
        raw = manager.create_client()
    except Exception as error:
        report.add(
            "kubernetes_config",
            "fail",
            f"Could not load the Kubernetes client configuration: {_message(error)}",
            "Check compute.kube_config_file and compute.kube_context, or in_cluster for a client in a Pod.",
        )
        report.skip_remaining("no Kubernetes client configuration")
        return
    try:
        selected = "in-cluster configuration" if config.in_cluster else f"context {config.kube_context}"
        report.add("kubernetes_config", "pass", f"Loaded {selected} for {raw.configuration.host}.")
        core = k8s.CoreV1Api(raw)
        if _check_namespace(report, core, namespace) and _check_service_account(
            report, core, namespace, config.server.service_account
        ):
            _check_access(report, k8s.AuthorizationV1Api(raw), config)
    finally:
        raw.close()


def _status(error) -> int | None:
    from kubernetes.client.exceptions import ApiException

    return error.status if isinstance(error, ApiException) else None


def _check_namespace(report, core, namespace) -> bool:
    """Return whether later cluster checks can still run."""
    try:
        found = core.read_namespace(namespace)
    except Exception as error:
        if _status(error) == 404:
            report.add(
                "namespace",
                "fail",
                f"Namespace {namespace} does not exist.",
                "Ask the platform owner to create it; this integration never creates namespaces.",
            )
            report.skip_remaining(f"namespace {namespace} does not exist")
            return False
        message, fatal = _failure(error)
        report.add(
            "namespace",
            "fail",
            f"Could not read namespace {namespace}: {message}",
            f"Grant the invoking identity get on namespaces/{namespace}: the wrapper records the "
            "namespace UID before creating compute and checks it before deleting compute."
            if _status(error) == 403
            else "Check that the cluster is reachable from this machine and the credentials are valid.",
        )
        if fatal:
            report.skip_remaining("the Kubernetes API did not answer")
        return not fatal
    if found.metadata.deletion_timestamp:
        report.add("namespace", "fail", f"Namespace {namespace} is terminating.", "Use an active namespace.")
        report.skip_remaining(f"namespace {namespace} is terminating")
        return False
    report.add("namespace", "pass", f"Namespace {namespace} exists.")
    return True


def _check_service_account(report, core, namespace, account) -> bool:
    """Return whether later cluster checks can still run."""
    try:
        core.read_namespaced_service_account(account, namespace)
    except Exception as error:
        if _status(error) == 404:
            report.add(
                "service_account",
                "fail",
                f"Driver service account {namespace}/{account} does not exist.",
                "Create it with a Role that lets the driver manage executor Pods, Services and "
                "ConfigMaps, or set compute.service_account.",
            )
            return True
        message, fatal = _failure(error)
        report.add(
            "service_account",
            "fail",
            f"Could not read service account {namespace}/{account}: {message}",
            "Grant the invoking identity get on serviceaccounts in the namespace; the wrapper reads "
            "it before creating compute.",
        )
        if fatal:
            report.skip_remaining("the Kubernetes API did not answer")
        return not fatal
    report.add(
        "service_account",
        "pass",
        f"Driver service account {namespace}/{account} exists; its own permissions are not verified.",
    )
    return True


def _check_access(report, authorization, config) -> None:
    namespace, account = config.server.namespace, config.server.service_account
    required = [*REQUIRED_ACCESS, *([] if config.in_cluster else [TUNNEL_ACCESS])]
    for verb, resource in required:
        check_id = permission_id(verb, resource)
        name = namespace if resource == "namespaces" else account if resource == "serviceaccounts" else None
        target = f"{verb} {resource}" + (f"/{name}" if name else "") + f" in namespace {namespace}"
        try:
            status = _review(authorization, namespace, verb, resource, name)
        except Exception as error:
            message, fatal = _failure(error)
            report.add(
                check_id,
                "fail",
                f"Could not verify {target}: {message}",
                "Retry, or ask an administrator to confirm it; an unverified permission is not assumed.",
            )
            if fatal:
                report.skip_remaining("the Kubernetes API did not answer")
                return
            continue
        if status is not None and status.allowed:
            report.add(check_id, "pass", f"Allowed: {target}.")
            continue
        detail = (status.evaluation_error or status.reason) if status is not None else None
        report.add(
            check_id,
            "fail",
            f"Denied: {target}" + (f" ({detail})." if detail else "."),
            f"Grant the invoking identity {verb} on {resource} in namespace {namespace}.",
        )
    if config.in_cluster:
        report.add(permission_id(*TUNNEL_ACCESS), "skip", "Not used: an in-cluster client needs no tunnel.")


def run_checks(
    report: CheckReport,
    *,
    project_dir: Path,
    profiles_dir: Path,
    profile: str | None = None,
    target: str | None = None,
    cli_vars: dict | None = None,
    timeout: float = 30,
) -> CheckReport:
    """Fill ``report`` in place, so an interrupted check can still be reported."""
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    credentials = _check_profile(report, project_dir, profiles_dir, profile, target, cli_vars or {})
    if credentials is None:
        report.skip_remaining("the profile is invalid")
        return report
    config = _check_compute(report, credentials)
    _check_client_libraries(report, credentials.method)
    if config is None:
        report.skip_remaining("the compute settings are invalid")
        return report
    _check_kubectl(report, config)
    _check_cluster(report, config, timeout)
    report.skip_remaining("not reached")
    return report
