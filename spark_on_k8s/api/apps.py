from __future__ import annotations

from fastapi import APIRouter
from kubernetes_asyncio.client import CoreV1Api
from pydantic import BaseModel

from spark_on_k8s.api import KubernetesClientSingleton
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.utils.app_manager import SparkAppStatus, get_app_status

router = APIRouter(
    prefix="/apps",
    tags=["spark-apps"],
)


class SparkApp(BaseModel):
    """App status."""

    app_id: str
    status: SparkAppStatus
    spark_ui_proxy: bool = False


@router.get("/list_apps")
async def list_apps_default_namespace() -> list[SparkApp]:
    """List spark apps in the default namespace."""
    return await list_apps(namespace=APIConfiguration.SPARK_ON_K8S_API_DEFAULT_NAMESPACE)


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
            status=get_app_status(pod),
            spark_ui_proxy=pod.metadata.labels.get("spark-ui-proxy", False),
        )
        for pod in driver_pods.items
    ]
