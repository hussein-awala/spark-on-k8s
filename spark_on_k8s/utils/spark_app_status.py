from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import kubernetes.client as k8s
    import kubernetes_asyncio.client as k8s_async


class SparkAppStatus(str, Enum):
    """Spark app status."""

    Pending = "Pending"
    Running = "Running"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Unknown = "Unknown"


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
