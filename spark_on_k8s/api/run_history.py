from __future__ import annotations

import asyncio
import logging
import time
from functools import lru_cache
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query
from kubernetes_asyncio.client import CoreV1Api
from starlette.responses import JSONResponse, Response

from spark_on_k8s.api import KubernetesClientSingleton
from spark_on_k8s.api.audit import metric_lines
from spark_on_k8s.api.namespace_scope import namespace_scope
from spark_on_k8s.history.collector import RunCollector, error_category
from spark_on_k8s.history.settings import HistorySettings
from spark_on_k8s.history.store import STATUSES, RunStore, timestamp

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Run history and operational health"])
READY_TIMEOUT = 5
_collector: RunCollector | None = None
# The readiness status this worker last logged; probes every few seconds only log changes.
_readiness: str | None = None


class NotReady(Exception):
    """Readiness failed; the message names namespaces and failure categories, never error text."""


@lru_cache(maxsize=1)
def get_history_settings():
    return HistorySettings.from_env()


async def start_history():
    global _collector
    settings = get_history_settings()
    if settings.enabled:
        store = RunStore(settings.path, retention_days=settings.retention_days, max_rows=settings.max_rows)
        _collector = RunCollector(settings, store, KubernetesClientSingleton.client)
        await _collector.start()


async def stop_history():
    global _collector
    collector, _collector = _collector, None
    if collector is not None:
        await collector.stop()


def _store():
    if not get_history_settings().enabled:
        raise HTTPException(404, detail={"code": "history_disabled", "message": "Run history is disabled."})
    if _collector is None:
        raise HTTPException(
            503, detail={"code": "history_unavailable", "message": "Run history is unavailable."}
        )
    return _collector.store


@router.get("/runs/v1/config")
async def history_config():
    store = _store()
    settings = get_history_settings()
    try:
        health = await asyncio.to_thread(store.health)
    except Exception as error:
        raise HTTPException(
            503, detail={"code": "history_unavailable", "message": "Run history is unavailable."}
        ) from error
    now = time.time()
    collection = []
    for namespace in settings.namespaces:
        state = health.get(namespace, {})
        collection.append(
            {
                "namespace": namespace,
                "fresh": settings.is_fresh(state.get("last_success"), now),
                "last_success": timestamp(state.get("last_success")),
                "last_attempt": timestamp(state.get("last_attempt")),
                "last_error": state.get("last_error"),
            }
        )
    return {
        "schema_version": 1,
        "enabled": True,
        "namespaces": settings.namespaces,
        "retention_days": settings.retention_days,
        "max_rows": settings.max_rows,
        "readiness": settings.readiness,
        "freshness_seconds": settings.freshness_seconds,
        "collection": collection,
    }


@router.get("/runs/v1")
async def list_runs(
    namespace: Annotated[str | None, Query(max_length=63)] = None,
    status: Literal["Pending", "Running", "Succeeded", "Failed", "Unknown"] | None = None,
    presence: Literal["all", "present", "missing"] = "all",
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
):
    store = _store()
    settings = get_history_settings()
    if namespace is not None and namespace not in settings.namespaces:
        raise HTTPException(
            403, detail={"code": "namespace_unavailable", "message": "Namespace is not recorded."}
        )
    try:
        result = await asyncio.to_thread(
            store.list_runs,
            namespace=namespace,
            status=status,
            presence=presence,
            limit=limit,
            cursor=cursor,
            allowed_namespaces=settings.namespaces,
        )
    except ValueError as error:
        raise HTTPException(
            400, detail={"code": "invalid_cursor", "message": "Invalid run-list cursor."}
        ) from error
    except Exception as error:
        raise HTTPException(
            503, detail={"code": "history_unavailable", "message": "Run history is unavailable."}
        ) from error
    return JSONResponse(result, headers={"Cache-Control": "no-store"})


async def _check_ready():
    """Return stale history namespaces that degrade readiness; raise NotReady if the API cannot serve."""
    settings = get_history_settings()
    checked_namespaces = settings.namespaces if settings.enabled else (namespace_scope().default,)
    api = CoreV1Api(await KubernetesClientSingleton.client())
    results = await asyncio.gather(
        *(
            api.list_namespaced_pod(
                namespace=namespace, label_selector="spark-role=driver", limit=1, _request_timeout=2
            )
            for namespace in checked_namespaces
        ),
        return_exceptions=True,
    )
    failed = [
        f"{namespace} ({error_category(result)})"
        for namespace, result in zip(checked_namespaces, results, strict=True)
        if isinstance(result, BaseException)
    ]
    if failed and (
        len(failed) == len(checked_namespaces) or not settings.enabled or settings.readiness == "strict"
    ):
        raise NotReady("Kubernetes pod listing failed in " + ", ".join(failed))
    if not settings.enabled:
        return []
    try:
        health = await asyncio.to_thread(_store().health)
    except Exception as error:
        raise NotReady("the run-history store is unavailable") from error
    now = time.time()
    stale = [
        f"{namespace} (last error: {health.get(namespace, {}).get('last_error') or 'none'})"
        for namespace in settings.namespaces
        if not settings.is_fresh(health.get(namespace, {}).get("last_success"), now)
    ]
    if len(stale) == len(settings.namespaces):
        raise NotReady("no run-history namespace has a fresh collection: " + ", ".join(stale))
    if stale and settings.readiness == "strict":
        raise NotReady("run-history collection is stale in " + ", ".join(stale))
    # Degraded mode tolerates pod-list failures in some namespaces until their collection becomes stale.
    return stale


def _log_readiness(status, detail=""):
    """Log readiness status changes once per worker; probes every few seconds do not repeat them."""
    global _readiness
    previous, _readiness = _readiness, status
    if status == previous:
        return
    if status == "ready":
        if previous is not None:
            logger.info("API readiness recovered")
    elif status == "degraded":
        logger.warning("API is ready but degraded: run-history collection is stale in %s", detail)
    else:
        logger.warning("API is not ready: %s", detail)


@router.get("/ready")
@router.head("/ready", include_in_schema=False)
async def ready():
    try:
        stale = await asyncio.wait_for(_check_ready(), timeout=READY_TIMEOUT)
    except NotReady as error:
        reason = str(error)
    except TimeoutError:
        reason = "the readiness check timed out"
    except Exception as error:
        reason = f"the readiness check failed ({error_category(error)})"
    else:
        # The probe is public in remote mode, so its body names no namespace or error.
        status = "degraded" if stale else "ready"
        _log_readiness(status, ", ".join(stale))
        return JSONResponse({"status": status}, headers={"Cache-Control": "no-store"})
    _log_readiness("not_ready", reason)
    return JSONResponse({"status": "not_ready"}, status_code=503, headers={"Cache-Control": "no-store"})


@router.get("/metrics")
async def metrics():
    settings = get_history_settings()
    if not settings.metrics_enabled:
        raise HTTPException(404, detail="Metrics are disabled.")
    lines = [
        "# HELP spark_on_k8s_history_enabled Whether optional operational history is configured.\n"
        "# TYPE spark_on_k8s_history_enabled gauge",
        f"spark_on_k8s_history_enabled {int(settings.enabled)}",
        *metric_lines(),
    ]
    if settings.enabled:
        try:
            store = _store()
            health, counts = await asyncio.gather(
                asyncio.to_thread(store.health), asyncio.to_thread(store.counts)
            )
        except Exception:
            return Response("Metrics unavailable\n", status_code=503, media_type="text/plain")
        by_status = {(row["namespace"], row["status"]): row["count"] for row in counts}
        # Each family's metadata and samples must form one group in the text exposition format.
        recorded = [
            "# HELP spark_on_k8s_runs_recorded Retained records by last observed phase; "
            "includes missing pods; "
            "not current capacity or an execution success-rate denominator.",
            "# TYPE spark_on_k8s_runs_recorded gauge",
        ]
        totals = {
            field: [f"# TYPE spark_on_k8s_collection_{field}_total counter"]
            for field in ("successes", "failures")
        }
        ages = [
            "# HELP spark_on_k8s_collection_age_seconds Seconds since last successful "
            "namespace observation; "
            "-1 means never observed.",
            "# TYPE spark_on_k8s_collection_age_seconds gauge",
        ]
        stale = [
            "# HELP spark_on_k8s_collection_stale 1 when the namespace has no successful collection "
            "within the readiness freshness window, otherwise 0.",
            "# TYPE spark_on_k8s_collection_stale gauge",
        ]
        now = time.time()
        for namespace in settings.namespaces:
            for phase in sorted(STATUSES):
                recorded.append(
                    f'spark_on_k8s_runs_recorded{{namespace="{namespace}",status="{phase}"}} '
                    f"{by_status.get((namespace, phase), 0)}"
                )
            state = health.get(namespace, {})
            for field, family in totals.items():
                family.append(
                    f'spark_on_k8s_collection_{field}_total{{namespace="{namespace}"}} {state.get(field, 0)}'
                )
            age = max(0, now - state["last_success"]) if state.get("last_success") is not None else -1
            ages.append(f'spark_on_k8s_collection_age_seconds{{namespace="{namespace}"}} {age:.3f}')
            fresh = settings.is_fresh(state.get("last_success"), now)
            stale.append(f'spark_on_k8s_collection_stale{{namespace="{namespace}"}} {int(not fresh)}')
        lines.extend(recorded + totals["successes"] + totals["failures"] + ages + stale)
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
