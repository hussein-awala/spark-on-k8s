from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import httpx
from fastapi import APIRouter, HTTPException, Query
from kubernetes_asyncio import client as k8s
from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from spark_on_k8s.api import AsyncHttpClientSingleton, KubernetesClientSingleton, logger
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.utils.console_models import (
    AIRFLOW_TASK_ANNOTATION,
    AirflowTaskContext,
    ConsoleApplication,
    ConsoleApplicationsPage,
    ConsoleDiagnostics,
    ConsoleHistory,
)
from spark_on_k8s.utils.spark_app_status import SparkAppStatus, get_app_status

router = APIRouter(prefix="/webserver/api", tags=["console"])


class _HistoryAttempt(BaseModel):
    startTime: str | None = None
    startTimeEpoch: int | None = None
    appSparkVersion: str | None = None


class _HistoryApplication(BaseModel):
    id: str = Field(min_length=1)
    name: str | None = None
    attempts: list[_HistoryAttempt] = Field(default_factory=list)


def _application_from_pod(pod: k8s.V1Pod, namespace: str) -> ConsoleApplication:
    labels = pod.metadata.labels or {}
    app_id = labels.get("spark-app-id")
    containers = pod.spec.containers if pod.spec else []
    task_context = None
    annotation = (pod.metadata.annotations or {}).get(AIRFLOW_TASK_ANNOTATION)
    if annotation:
        try:
            task_context = AirflowTaskContext.model_validate_json(annotation)
        except ValidationError:
            pass
    return ConsoleApplication(
        app_id=app_id or pod.metadata.name,
        name=labels.get("spark-app-name") or app_id or pod.metadata.name,
        namespace=pod.metadata.namespace or namespace,
        status=get_app_status(pod) if pod.status else SparkAppStatus.Unknown,
        created_at=pod.metadata.creation_timestamp,
        driver_pod=pod.metadata.name,
        image=containers[0].image if containers else None,
        spark_version=labels.get("spark-version"),
        node_name=pod.spec.node_name if pod.spec else None,
        # Existing log/action endpoints target spark-app-id, not a pod-name fallback.
        driver_logs=bool(app_id),
        spark_ui_proxy=bool(app_id) and labels.get("spark-ui-proxy", "").lower() == "true",
        source="kubernetes",
        task_context=task_context,
    )


def _kubernetes_error(error: k8s.ApiException) -> HTTPException:
    messages = {
        403: "Access to the requested Kubernetes resources is forbidden.",
        404: "The requested Kubernetes resource was not found.",
        410: "The application page expired. Reload the application list.",
        429: "Kubernetes is busy. Retry the request shortly.",
    }
    status = error.status if error.status in messages else 502
    return HTTPException(
        status_code=status,
        detail={"code": "kubernetes_error", "message": messages.get(status, "Kubernetes is unavailable.")},
    )


@router.get("/apps", response_model=ConsoleApplicationsPage)
async def list_applications(
    namespace: Annotated[
        str | None, Query(min_length=1, max_length=63, pattern=r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    continue_from: str = "",
) -> ConsoleApplicationsPage:
    """List one Kubernetes page without requiring cluster-wide namespace access."""
    if continue_from == "no-more-items":
        return ConsoleApplicationsPage(items=[])
    namespace = namespace or APIConfiguration.SPARK_ON_K8S_API_DEFAULT_NAMESPACE
    try:
        client = k8s.CoreV1Api(await KubernetesClientSingleton.client())
        options = {"namespace": namespace, "label_selector": "spark-role=driver", "limit": limit}
        if continue_from:
            options["_continue"] = continue_from
        pods = await client.list_namespaced_pod(**options)
    except k8s.ApiException as error:
        logger.exception("Console Kubernetes application listing failed")
        raise _kubernetes_error(error) from error
    except Exception as error:
        logger.exception("Console Kubernetes client failed")
        raise HTTPException(
            status_code=502,
            detail={"code": "kubernetes_unavailable", "message": "Kubernetes is unavailable."},
        ) from error
    return ConsoleApplicationsPage(
        items=[_application_from_pod(pod, namespace) for pod in pods.items],
        next_cursor=pods.metadata._continue or None,
    )


def _history_start_time(attempt: _HistoryAttempt) -> datetime | None:
    try:
        if attempt.startTimeEpoch is not None:
            return datetime.fromtimestamp(attempt.startTimeEpoch / 1000, tz=UTC)
        if attempt.startTime:
            timestamp = attempt.startTime.removesuffix("GMT")
            parsed = datetime.fromisoformat(timestamp)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except (ValueError, OverflowError, OSError):
        pass
    return None


@router.get("/history", response_model=ConsoleHistory)
async def list_history() -> ConsoleHistory:
    """List completed history records independently of live Kubernetes applications."""
    host = APIConfiguration.SPARK_ON_K8S_API_SPARK_HISTORY_HOST
    if not host:
        raise HTTPException(
            status_code=412,
            detail={"code": "history_not_configured", "message": "Spark History Server is not configured."},
        )
    base_url = host if "://" in host else f"http://{host}"
    try:
        response = await AsyncHttpClientSingleton.client().get(
            f"{base_url.rstrip('/')}/api/v1/applications", params={"status": "completed"}
        )
        response.raise_for_status()
        applications = TypeAdapter(list[_HistoryApplication]).validate_python(response.json())
    except (httpx.HTTPError, httpx.InvalidURL, ValueError) as error:
        logger.exception("Console Spark History Server listing failed")
        raise HTTPException(
            status_code=502,
            detail={"code": "history_unavailable", "message": "Spark History Server is unavailable."},
        ) from error
    items = []
    for application in applications:
        attempt = application.attempts[0] if application.attempts else None
        items.append(
            ConsoleApplication(
                app_id=application.id,
                name=application.name or application.id,
                status=SparkAppStatus.Unknown,
                created_at=_history_start_time(attempt) if attempt else None,
                spark_version=attempt.appSparkVersion if attempt else None,
                spark_history_proxy=True,
                source="history",
            )
        )
    return ConsoleHistory(items=items)


@router.get("/apps/{namespace}/pods/{driver_pod}", response_model=ConsoleApplication)
async def get_application_by_pod(namespace: str, driver_pod: str) -> ConsoleApplication:
    """Resolve an existing driver reference without deriving a potentially truncated app ID."""
    try:
        client = k8s.CoreV1Api(await KubernetesClientSingleton.client())
        pod = await client.read_namespaced_pod(namespace=namespace, name=driver_pod)
    except k8s.ApiException as error:
        logger.exception("Console Kubernetes driver lookup failed")
        raise _kubernetes_error(error) from error
    except Exception as error:
        logger.exception("Console Kubernetes client failed")
        raise HTTPException(
            status_code=502,
            detail={"code": "kubernetes_unavailable", "message": "Kubernetes is unavailable."},
        ) from error
    if (pod.metadata.labels or {}).get("spark-role") != "driver":
        raise HTTPException(
            status_code=404,
            detail={"code": "application_not_found", "message": "The Spark driver was not found."},
        )
    return _application_from_pod(pod, namespace)


@router.get("/apps/{namespace}/pods/{driver_pod}/diagnostics", response_model=ConsoleDiagnostics)
async def diagnose_application(
    namespace: str,
    driver_pod: str,
    pod_limit: Annotated[int, Query(ge=1, le=50)] = 20,
    event_limit: Annotated[int, Query(ge=1, le=200)] = 100,
) -> ConsoleDiagnostics:
    from spark_on_k8s.diagnostics import collect_diagnosis_async, validate_target

    try:
        validate_target(namespace, driver_pod)
    except ValueError as error:
        raise HTTPException(400, detail={"code": "invalid_target", "message": str(error)}) from error
    try:
        client = k8s.CoreV1Api(await KubernetesClientSingleton.client())
        pod = await client.read_namespaced_pod(namespace=namespace, name=driver_pod, _request_timeout=5)
        if (pod.metadata.labels or {}).get("spark-role") != "driver":
            raise HTTPException(
                404, detail={"code": "application_not_found", "message": "The Spark driver was not found."}
            )
        report = await collect_diagnosis_async(
            client,
            pod,
            namespace=namespace,
            pod_limit=pod_limit,
            event_limit=event_limit,
        )
    except k8s.ApiException as error:
        raise _kubernetes_error(error) from error
    except ValueError as error:
        raise HTTPException(
            409,
            detail={
                "code": "identity_unavailable",
                "message": "The driver has no usable Spark application identity.",
            },
        ) from error
    except HTTPException:
        raise
    except Exception as error:
        raise HTTPException(
            502,
            detail={"code": "diagnostics_unavailable", "message": "Kubernetes diagnostics are unavailable."},
        ) from error
    return ConsoleDiagnostics(application=_application_from_pod(pod, namespace), diagnosis=report)
