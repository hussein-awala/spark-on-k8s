from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import kubernetes.client as k8s
    import kubernetes_asyncio.client as k8s_async


class SparkAppStatus(StrEnum):
    """Spark app status."""

    Pending = "Pending"
    Running = "Running"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Unknown = "Unknown"


# Consecutive failed status reads a wait accepts when each failure may clear on its own
# (throttling, a server error or a lost connection) before it reports the last one.
TRANSIENT_STATUS_READ_ATTEMPTS = 5


def is_transient_status_code(status: int | None) -> bool:
    """Whether a Kubernetes API response status may clear on its own."""
    return status == 429 or (status is not None and 500 <= status <= 599)


def get_app_status(pod: k8s.V1Pod | k8s_async.V1Pod) -> SparkAppStatus:
    """Get app status."""
    if pod.status.phase == "Pending":
        return SparkAppStatus.Pending
    elif pod.status.phase == "Running":
        return SparkAppStatus.Running
    elif pod.status.phase == "Succeeded":
        return SparkAppStatus.Succeeded
    elif pod.status.phase == "Failed":
        return SparkAppStatus.Failed
    else:
        return SparkAppStatus.Unknown


def driver_pod_selector(app_id: str) -> str:
    """Select only driver pods: other pods may carry an application's ID label."""
    return f"spark-app-id={app_id},spark-role=driver"


def select_driver_pod(pods: list, app_id: str):
    """Return the only driver for an application ID instead of acting on an arbitrary match."""
    if len(pods) == 0:
        raise ValueError(f"No pods found for app {app_id}")
    if len(pods) > 1:
        raise ValueError(f"Multiple driver pods found for app {app_id}; use a run receipt")
    return pods[0]


def driver_container_name(pod: k8s.V1Pod | k8s_async.V1Pod) -> str:
    """Name the submitted driver container, even if an admission webhook prepended a sidecar."""
    names = [container.name for container in pod.spec.containers]
    metadata = getattr(pod, "metadata", None)
    selected = (getattr(metadata, "annotations", None) or {}).get("kubectl.kubernetes.io/default-container")
    for candidate in (selected, "driver"):
        if candidate in names:
            return candidate
    return names[0]
