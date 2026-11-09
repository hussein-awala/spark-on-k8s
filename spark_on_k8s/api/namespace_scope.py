from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import HTTPException

from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.utils.configuration import Configuration

if TYPE_CHECKING:
    from collections.abc import Iterable

_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
SERVICE_ACCOUNT_NAMESPACE = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")


@cache
def current_namespace() -> str:
    """Resolve the namespace kubectl would use: the pod's own, else the kubeconfig context's."""
    if Configuration.SPARK_ON_K8S_IN_CLUSTER:
        try:
            namespace = SERVICE_ACCOUNT_NAMESPACE.read_text().strip()
        except OSError as error:
            raise ValueError(
                "The current namespace is unavailable without a mounted service account; "
                "set SPARK_ON_K8S_API_NAMESPACES explicitly."
            ) from error
        if not _DNS_LABEL.fullmatch(namespace):
            raise ValueError("The mounted service account namespace is not a valid namespace name.")
        return namespace
    from kubernetes import config

    try:
        contexts, active = config.list_kube_config_contexts(
            config_file=Configuration.SPARK_ON_K8S_CONFIG_FILE
        )
    except (config.ConfigException, OSError):
        return "default"
    if Configuration.SPARK_ON_K8S_CONTEXT:
        active = next(
            (context for context in contexts if context.get("name") == Configuration.SPARK_ON_K8S_CONTEXT),
            None,
        )
    return ((active or {}).get("context") or {}).get("namespace") or "default"


@dataclass(frozen=True)
class NamespaceScope:
    """Namespaces the API serves; ``namespaces`` is None when every namespace is in scope."""

    namespaces: tuple[str, ...] | None
    default: str

    def allows(self, namespace: str) -> bool:
        return self.namespaces is None or namespace in self.namespaces

    def require(self, namespace: str) -> str:
        """Reject a request target before any Kubernetes call or Spark UI connection."""
        if not self.allows(namespace):
            raise HTTPException(
                403,
                detail={
                    "code": "namespace_forbidden",
                    "message": "The namespace is outside this API's namespace scope.",
                },
            )
        return namespace

    def require_configured(self, namespaces: Iterable[str], setting: str) -> None:
        """Fail at startup when another setting names a namespace the API would refuse."""
        outside = [namespace for namespace in namespaces if not self.allows(namespace)]
        if outside:
            raise ValueError(
                f"{setting} names namespaces outside SPARK_ON_K8S_API_NAMESPACES: {', '.join(outside)}"
            )


def parse_namespaces(value: str) -> tuple[str, ...] | None:
    """Parse empty (the current namespace), ``*`` (every namespace) or a comma-separated list."""
    value = value.strip()
    if value == "*":
        return None
    if not value:
        return (current_namespace(),)
    namespaces = tuple(dict.fromkeys(part.strip() for part in value.split(",")))
    if not all(_DNS_LABEL.fullmatch(namespace) for namespace in namespaces):
        raise ValueError(
            "SPARK_ON_K8S_API_NAMESPACES must be empty for the current namespace, "
            '"*" for every namespace, or comma-separated namespace names.'
        )
    return namespaces


def namespace_scope() -> NamespaceScope:
    namespaces = parse_namespaces(APIConfiguration.SPARK_ON_K8S_API_NAMESPACES)
    default = APIConfiguration.SPARK_ON_K8S_API_DEFAULT_NAMESPACE or (
        namespaces[0] if namespaces else current_namespace()
    )
    scope = NamespaceScope(namespaces, default)
    if not scope.allows(default):
        raise ValueError("SPARK_ON_K8S_API_DEFAULT_NAMESPACE is outside SPARK_ON_K8S_API_NAMESPACES.")
    return scope
