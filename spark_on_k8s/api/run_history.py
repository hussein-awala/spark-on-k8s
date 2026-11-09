from __future__ import annotations

import asyncio
import time
from functools import lru_cache
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query
from kubernetes_asyncio.client import CoreV1Api
from starlette.responses import JSONResponse, Response

from spark_on_k8s.api import KubernetesClientSingleton
from spark_on_k8s.api.audit import metric_lines
from spark_on_k8s.api.namespace_scope import namespace_scope
from spark_on_k8s.history.collector import RunCollector
from spark_on_k8s.history.settings import HistorySettings
from spark_on_k8s.history.store import STATUSES, RunStore

router = APIRouter(tags=["Run history and operational health"])
READY_TIMEOUT = 5
_collector: RunCollector | None = None


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
    _store()
    settings = get_history_settings()
    return {
        "schema_version": 1,
        "enabled": True,
        "namespaces": settings.namespaces,
        "retention_days": settings.retention_days,
        "max_rows": settings.max_rows,
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
    settings = get_history_settings()
    checked_namespaces = settings.namespaces if settings.enabled else (namespace_scope().default,)
    api = CoreV1Api(await KubernetesClientSingleton.client())
    await asyncio.gather(
        *(
            api.list_namespaced_pod(
                namespace=namespace, label_selector="spark-role=driver", limit=1, _request_timeout=2
            )
            for namespace in checked_namespaces
        )
    )
    if settings.enabled:
        health = await asyncio.to_thread(_store().health)
        now = time.time()
        if any(
            not health.get(namespace, {}).get("last_success")
            or not 0 <= now - health[namespace]["last_success"] <= settings.freshness_seconds
            for namespace in settings.namespaces
        ):
            raise RuntimeError("Run history is not fresh")


@router.get("/ready")
@router.head("/ready", include_in_schema=False)
async def ready():
    try:
        await asyncio.wait_for(_check_ready(), timeout=READY_TIMEOUT)
    except Exception:
        return JSONResponse({"status": "not_ready"}, status_code=503, headers={"Cache-Control": "no-store"})
    return JSONResponse({"status": "ready"}, headers={"Cache-Control": "no-store"})


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
            age = max(0, time.time() - state["last_success"]) if state.get("last_success") is not None else -1
            ages.append(f'spark_on_k8s_collection_age_seconds{{namespace="{namespace}"}} {age:.3f}')
        lines.extend(recorded + totals["successes"] + totals["failures"] + ages)
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
