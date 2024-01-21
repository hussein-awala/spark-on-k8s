from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from fastapi import APIRouter
from kubernetes_asyncio.client import CoreV1Api
from pydantic import BaseModel

from spark_on_k8s.api import KubernetesClientSingleton

if TYPE_CHECKING:
    from kubernetes_asyncio.client import V1Pod

router = APIRouter(
    prefix="/apps",
    tags=["spark-apps"],
)


class SparkAppStatus(str, Enum):
    """Spark app status."""

    Pending = "Pending"
    Running = "Running"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Unknown = "Unknown"


class SparkApp(BaseModel):
    """App status."""

    app_id: str
    status: SparkAppStatus
    spark_ui_proxy: bool = False


def _get_app_status(pod: V1Pod) -> SparkAppStatus:
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


@router.get("/list_apps/{namespace}")
async def list_apps(namespace: str) -> list[SparkApp]:
    """List spark apps in a namespace."""
    core_client = CoreV1Api(await KubernetesClientSingleton.client())
    driver_pods = await core_client.list_namespaced_pod(
        namespace=namespace, label_selector="spark-role=driver"
    )
    return [
        SparkApp(
            app_id=pod.metadata.labels.get("spark-app-id", pod.metadata.name),
            status=_get_app_status(pod),
            spark_ui_proxy=pod.metadata.labels.get("spark-ui-proxy", False),
        )
        for pod in driver_pods.items
    ]
