from __future__ import annotations

from fastapi import APIRouter

# ruff: noqa: TCH002
from fastapi.responses import Response
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
    driver_logs: bool = False
    spark_ui_proxy: bool = False
    spark_history_proxy: bool = False


async def _list_apps(namespace: str, max_per_page: int, continue_from: str) -> tuple[str, list[SparkApp]]:
    no_more_items = "no-more-items"
    if continue_from == no_more_items:
        return no_more_items, []
    core_client = CoreV1Api(await KubernetesClientSingleton.client())
    search_params = {
        "namespace": namespace,
        "label_selector": "spark-role=driver",
        "limit": max_per_page,
    }
    if continue_from:
        search_params["_continue"] = continue_from
    driver_pods = await core_client.list_namespaced_pod(**search_params)
    return (
        driver_pods.metadata._continue if driver_pods.metadata._continue else no_more_items,
        [
            SparkApp(
                app_id=pod.metadata.labels.get("spark-app-id", pod.metadata.name),
                status=get_app_status(pod),
                driver_logs=True,
                spark_ui_proxy=pod.metadata.labels.get("spark-ui-proxy", False),
            )
            for pod in driver_pods.items
        ],
    )


@router.get("/list_apps")
async def list_apps_default_namespace(
    response: Response, max_per_page: int = 10, continue_from: str = ""
) -> list[SparkApp]:
    """List spark apps in the default namespace."""
    continue_from, apps = await _list_apps(
        namespace=APIConfiguration.SPARK_ON_K8S_API_DEFAULT_NAMESPACE,
        max_per_page=max_per_page,
        continue_from=continue_from,
    )
    response.headers["X-Continue"] = continue_from
    return apps


@router.get("/list_apps/{namespace}")
async def list_apps(
    response: Response, namespace: str, max_per_page: int = 10, continue_from: str = ""
) -> list[SparkApp]:
    """List spark apps in a namespace."""
    continue_from, apps = await _list_apps(
        namespace=namespace,
        max_per_page=max_per_page,
        continue_from=continue_from,
    )
    response.headers["X-Continue"] = continue_from
    return apps
