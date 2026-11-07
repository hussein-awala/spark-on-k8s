from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
from typing import Any

from kubernetes import client as k8s
from urllib3.exceptions import TimeoutError as HTTPTimeoutError

from spark_on_k8s.diagnostics import DiagnosticFinding, validate_target
from spark_on_k8s.utils.run import WaitDeadline

# Checks describe the authenticated submitter, never an impersonated driver identity.
CALLER_CHECKS = (
    ("pods", "create"),
    ("pods", "get"),
    ("pods", "list"),
    ("pods", "delete"),
    ("pods/log", "get"),
    ("services", "create"),
    ("configmaps", "create"),
    ("events", "list"),
)


@dataclass
class DoctorReport:
    namespace: str
    service_account: str
    findings: list[DiagnosticFinding] = field(default_factory=list)
    caller_permissions: list[dict[str, Any]] = field(default_factory=list)
    driver_permissions_verified: bool = False
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def driver_rbac(namespace: str, service_account: str) -> list[dict]:
    """Render a namespace-scoped baseline driver role; never apply it."""
    validate_target(namespace, service_account)
    role_name = f"{service_account}-spark-driver"
    if len(role_name) > 253:
        digest = sha256(service_account.encode()).hexdigest()[:10]
        role_name = f"{service_account[:229]}-{digest}-spark-driver"
    return [
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {"name": role_name, "namespace": namespace},
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["pods"],
                    "verbs": ["get", "list", "watch", "create", "patch", "delete"],
                },
                {
                    "apiGroups": [""],
                    "resources": ["services", "configmaps"],
                    "verbs": ["get", "list", "watch", "create", "delete", "deletecollection"],
                },
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": role_name, "namespace": namespace},
            "subjects": [{"kind": "ServiceAccount", "name": service_account, "namespace": namespace}],
            "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role_name},
        },
    ]


def check_cluster(client, *, namespace: str, service_account: str, timeout: float = 10) -> DoctorReport:
    """Read service-account metadata and perform non-persistent self-access reviews."""
    validate_target(namespace, service_account)
    if timeout is None or timeout <= 0 or timeout > 30:
        raise ValueError("timeout must be between 0 (exclusive) and 30 seconds")
    deadline = WaitDeadline(timeout)
    report = DoctorReport(namespace, service_account)
    api, authorization = k8s.CoreV1Api(client), k8s.AuthorizationV1Api(client)
    try:
        account = api.read_namespaced_service_account(
            name=service_account,
            namespace=namespace,
            **deadline.request_options(),
        )
        report.findings.append(
            DiagnosticFinding(
                "service_account_found",
                "info",
                service_account,
                "Driver service account exists",
                f"ServiceAccount {namespace}/{account.metadata.name}",
                "Existence does not establish its permissions; ask an administrator to review the "
                "scoped driver role.",
            )
        )
        if account.automount_service_account_token is False:
            report.findings.append(
                DiagnosticFinding(
                    "service_account_token_disabled",
                    "warning",
                    service_account,
                    "Automatic API credentials are disabled",
                    "ServiceAccount automountServiceAccountToken is false.",
                    "Confirm that the driver receives an explicit projected token or another "
                    "supported Kubernetes credential.",
                )
            )
    except (k8s.ApiException, TimeoutError, HTTPTimeoutError) as error:
        code = getattr(error, "status", "timeout")
        report.findings.append(
            DiagnosticFinding(
                "service_account_unavailable",
                "warning",
                service_account,
                "Driver service account could not be verified",
                f"Service-account lookup returned {code}.",
                "Check the namespace and service-account name, and the caller's permission to "
                "read serviceaccounts.",
            )
        )
    for resource, verb in CALLER_CHECKS:
        name, _, subresource = resource.partition("/")
        check = {"resource": resource, "verb": verb, "allowed": None}
        try:
            review = authorization.create_self_subject_access_review(
                body=k8s.V1SelfSubjectAccessReview(
                    spec=k8s.V1SelfSubjectAccessReviewSpec(
                        resource_attributes=k8s.V1ResourceAttributes(
                            namespace=namespace,
                            group="",
                            resource=name,
                            subresource=subresource or None,
                            verb=verb,
                        ),
                    )
                ),
                **deadline.request_options(),
            )
            check["allowed"] = bool(review.status.allowed) if review.status else None
        except (k8s.ApiException, TimeoutError, HTTPTimeoutError):
            pass
        report.caller_permissions.append(check)
        if check["allowed"] is not True:
            report.findings.append(
                DiagnosticFinding(
                    "caller_permission_denied" if check["allowed"] is False else "caller_permission_unknown",
                    "warning",
                    resource,
                    f"Caller permission: {verb} {resource}",
                    "Denied by Kubernetes authorization."
                    if check["allowed"] is False
                    else "Access review unavailable.",
                    "Review the caller's namespace-scoped role. These checks do not test or grant "
                    "driver service-account permissions.",
                )
            )
    return report
