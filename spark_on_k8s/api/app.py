from __future__ import annotations

from fastapi import APIRouter, Response

from spark_on_k8s.api import KubernetesClientSingleton
from spark_on_k8s.api.utils import handle_exception
from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager

router = APIRouter(
    prefix="/app",
    tags=["spark-app"],
)


@router.patch(
    "/{namespace}/{app_id}",
    summary="Kill a spark application",
)
async def kill_app(namespace: str, app_id: str):
    """This endpoint kills a spark application by its app_id.

    It sends a SIGTERM to the init process of the driver pod (PID 1).
    """
    async_spark_app_manager = AsyncSparkAppManager(
        k8s_client_manager=KubernetesClientSingleton.client_manager
    )
    try:
        await async_spark_app_manager.kill_app(namespace=namespace, app_id=app_id)
        return Response(status_code=200)
    except Exception as e:
        # TODO: handle exceptions properly and return proper status code
        return handle_exception(e, 500)


@router.delete(
    "/{namespace}/{app_id}",
    summary="Delete a spark application",
)
async def delete_app(namespace: str, app_id: str, force: bool = False):
    """This endpoint deletes a spark application by its app_id.

    It deletes the driver pod, then the Kubernetes garbage collector
    will delete the executor pods and the other resources.
    """
    async_spark_app_manager = AsyncSparkAppManager(
        k8s_client_manager=KubernetesClientSingleton.client_manager
    )
    try:
        await async_spark_app_manager.delete_app(namespace=namespace, app_id=app_id)
        return Response(status_code=200)
    except Exception as e:
        # TODO: handle exceptions properly and return proper status code
        return handle_exception(e, 500)
