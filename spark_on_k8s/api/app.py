from __future__ import annotations

from fastapi import APIRouter, HTTPException, Response
from kubernetes_asyncio.client import ApiException

from spark_on_k8s.api import KubernetesClientSingleton
from spark_on_k8s.api.audit import count_kubernetes_error
from spark_on_k8s.api.namespace_scope import namespace_scope
from spark_on_k8s.api.utils import handle_exception
from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager

router = APIRouter(
    prefix="/app",
    tags=["spark-app"],
)

_TARGET_ERRORS = {
    # Kubernetes refuses an identifier it cannot use as a label selector value: nothing changed.
    400: ("invalid_request", "Kubernetes rejected the application identifier."),
    422: ("invalid_request", "Kubernetes rejected the application identifier."),
    403: ("forbidden", "Access to the Spark application is forbidden."),
    404: ("application_not_found", "The Spark application was not found."),
    409: ("application_state", "The Spark application cannot be changed in its current state."),
}


def _target_error(e: Exception) -> HTTPException | None:
    """Report a missing, finished or ambiguous target without exposing Kubernetes details."""
    # The manager raises ValueError when the driver is missing, not running or ambiguous.
    status = e.status if isinstance(e, ApiException) else 409 if isinstance(e, ValueError) else None
    if status not in _TARGET_ERRORS:
        return None
    code, message = _TARGET_ERRORS[status]
    return HTTPException(
        status_code=400 if status == 422 else status, detail={"code": code, "message": message}
    )


@router.patch(
    "/{namespace}/{app_id}",
    summary="Kill a spark application",
)
async def kill_app(namespace: str, app_id: str):
    """This endpoint kills a spark application by its app_id.

    It sends a SIGTERM to the init process of the driver pod (PID 1).
    """
    namespace_scope().require(namespace)
    async_spark_app_manager = AsyncSparkAppManager(
        k8s_client_manager=KubernetesClientSingleton.client_manager
    )
    try:
        await async_spark_app_manager.kill_app(namespace=namespace, app_id=app_id)
        return Response(status_code=200)
    except Exception as e:
        count_kubernetes_error(e)
        if (error := _target_error(e)) is not None:
            raise error from e
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
    namespace_scope().require(namespace)
    async_spark_app_manager = AsyncSparkAppManager(
        k8s_client_manager=KubernetesClientSingleton.client_manager
    )
    try:
        await async_spark_app_manager.delete_app(namespace=namespace, app_id=app_id, force=force)
        return Response(status_code=200)
    except Exception as e:
        count_kubernetes_error(e)
        if (error := _target_error(e)) is not None:
            raise error from e
        return handle_exception(e, 500)
