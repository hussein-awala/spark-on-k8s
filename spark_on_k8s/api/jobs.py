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
    prefix="/jobs",
    tags=["spark-jobs"],
)


class SparkJobStatus(str, Enum):
    """Spark job status."""

    Pending = "Pending"
    Running = "Running"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Unknown = "Unknown"


class SparkJob(BaseModel):
    """Job status."""

    job_id: str
    status: SparkJobStatus
    spark_ui_proxy: bool = False


def _get_job_status(pod: V1Pod) -> SparkJobStatus:
    """Get job status."""
    if pod.status.phase == "Pending":
        return SparkJobStatus.Pending
    elif pod.status.phase == "Running":
        return SparkJobStatus.Running
    elif pod.status.phase == "Succeeded":
        return SparkJobStatus.Succeeded
    elif pod.status.phase == "Failed":
        return SparkJobStatus.Failed
    else:
        return SparkJobStatus.Unknown


@router.get("/list_jobs/{namespace}")
async def list_jobs(namespace: str) -> list[SparkJob]:
    """List spark jobs in a namespace."""
    core_client = CoreV1Api(await KubernetesClientSingleton.client())
    driver_pods = await core_client.list_namespaced_pod(
        namespace=namespace, label_selector="spark-role=driver"
    )
    return [
        SparkJob(
            job_id=pod.metadata.labels.get("spark-app-id", pod.metadata.name),
            status=_get_job_status(pod),
            spark_ui_proxy=pod.metadata.labels.get("spark-ui-proxy", False),
        )
        for pod in driver_pods.items
    ]
